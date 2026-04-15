"""Strava API data extractor.

Credentials are read from environment variables:
    STRAVA_CLIENT_ID      - your Strava app's client ID
    STRAVA_CLIENT_SECRET  - your Strava app's client secret
    STRAVA_REFRESH_TOKEN  - a valid refresh token for the athlete

Rate limits (Strava read endpoints):
    100 requests / 15 minutes
    1,000 requests / day

The extractor reads X-RateLimit-Usage / X-RateLimit-Limit headers after every
response and proactively sleeps before the window is exhausted rather than
waiting for a 429.  This keeps the daily budget intact and avoids losing
in-progress fetches.
"""

from __future__ import annotations

import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import httpx

REPO_ROOT = Path(__file__).resolve().parents[3]
STRAVA_TOKEN_URL = "https://www.strava.com/oauth/token"
STRAVA_ACTIVITIES_URL = "https://www.strava.com/api/v3/athlete/activities"
STRAVA_ACTIVITY_URL = "https://www.strava.com/api/v3/activities/{activity_id}"
STRAVA_ATHLETE_URL = "https://www.strava.com/api/v3/athlete"
DEFAULT_CACHE_ROOT = REPO_ROOT / "logs" / "strava_api"

DEFAULT_PER_PAGE = 200

# Stop this many requests before the 15-min limit so we never actually 429.
_RATE_LIMIT_BUFFER = 5
# Strava's read rate limits (conservative defaults; updated from headers at runtime).
_DEFAULT_LIMIT_15MIN = 100
_DEFAULT_LIMIT_DAILY = 1000
_WINDOW_SECONDS = 15 * 60


class StravaExtractor:
    def __init__(
        self,
        client_id: str | None = None,
        client_secret: str | None = None,
        refresh_token: str | None = None,
        cache_root: str | Path | None = None,
        cache_enabled: bool = True,
    ) -> None:
        self.client_id = client_id or os.environ["STRAVA_CLIENT_ID"]
        self.client_secret = client_secret or os.environ["STRAVA_CLIENT_SECRET"]
        self.refresh_token = refresh_token or os.environ["STRAVA_REFRESH_TOKEN"]
        self._access_token: str | None = None
        self.cache_enabled = cache_enabled
        self.cache_root = Path(cache_root) if cache_root is not None else DEFAULT_CACHE_ROOT
        self.cache_run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

        # Rate-limit state (populated from response headers)
        self._limit_15min: int = _DEFAULT_LIMIT_15MIN
        self._limit_daily: int = _DEFAULT_LIMIT_DAILY
        self._usage_15min: int = 0
        self._usage_daily: int = 0
        self._window_start: float = time.monotonic()

    @property
    def cache_run_dir(self) -> Path:
        return self.cache_root / self.cache_run_id

    def _write_cache_json(self, relative_path: str | Path, payload: Any) -> Path | None:
        if not self.cache_enabled:
            return None
        path = self.cache_run_dir / relative_path
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload, indent=2, sort_keys=True, default=str), encoding="utf-8")
        return path

    @staticmethod
    def _load_cached_json(path: str | Path) -> Any:
        return json.loads(Path(path).read_text(encoding="utf-8"))

    @classmethod
    def load_cached_activities(cls, cache_dir: str | Path) -> list[dict[str, Any]]:
        cache_path = Path(cache_dir)
        seen_ids: set[int] = set()
        activities: list[dict[str, Any]] = []

        for page_path in sorted((cache_path / "activities").glob("page-*.json")):
            payload = cls._load_cached_json(page_path)
            raw_activities = payload.get("raw_activities", payload if isinstance(payload, list) else [])
            if not isinstance(raw_activities, list):
                continue

            for item in raw_activities:
                if not isinstance(item, dict) or "id" not in item:
                    continue
                activity_id = int(item["id"])
                if activity_id in seen_ids:
                    continue
                seen_ids.add(activity_id)
                activities.append(_parse_activity(item))

        return activities

    @classmethod
    def load_cached_activity_detail(cls, activity_id: int, cache_dir: str | Path) -> dict[str, Any]:
        path = Path(cache_dir) / "activity_details" / f"{int(activity_id)}.json"
        payload = cls._load_cached_json(path)
        if not isinstance(payload, dict):
            raise ValueError(f"Cached activity detail at {path} is not a JSON object.")
        return payload

    @classmethod
    def load_cached_laps(cls, activity_id: int, cache_dir: str | Path) -> list[dict[str, Any]]:
        detail = cls.load_cached_activity_detail(activity_id, cache_dir)
        return [_parse_lap(activity_id, lap) for lap in detail.get("laps", [])]

    # ------------------------------------------------------------------
    # Auth
    # ------------------------------------------------------------------

    def _refresh_access_token(self) -> str:
        response = httpx.post(
            STRAVA_TOKEN_URL,
            data={
                "client_id": self.client_id,
                "client_secret": self.client_secret,
                "refresh_token": self.refresh_token,
                "grant_type": "refresh_token",
            },
        )
        response.raise_for_status()
        payload = response.json()
        self._access_token = payload["access_token"]
        self.refresh_token = payload.get("refresh_token", self.refresh_token)
        return self._access_token

    def _get_access_token(self) -> str:
        if self._access_token is None:
            self._refresh_access_token()
        return self._access_token  # type: ignore[return-value]

    def _auth_headers(self) -> dict[str, str]:
        return {"Authorization": f"Bearer {self._get_access_token()}"}

    # ------------------------------------------------------------------
    # Rate limit handling
    # ------------------------------------------------------------------

    def _update_usage(self, response: httpx.Response) -> None:
        """Parse X-RateLimit-* headers and update internal counters."""
        usage_str = response.headers.get("X-RateLimit-Usage", "")
        limit_str = response.headers.get("X-RateLimit-Limit", "")
        if usage_str:
            parts = usage_str.split(",")
            self._usage_15min = int(parts[0])
            if len(parts) > 1:
                self._usage_daily = int(parts[1])
        if limit_str:
            parts = limit_str.split(",")
            self._limit_15min = int(parts[0])
            if len(parts) > 1:
                self._limit_daily = int(parts[1])

    def _check_and_throttle(self) -> None:
        """Sleep if we are within the buffer of the 15-min limit.

        Called after a successful response so the next request stays safe.
        """
        remaining = self._limit_15min - self._usage_15min
        if remaining <= _RATE_LIMIT_BUFFER:
            elapsed = time.monotonic() - self._window_start
            wait = max(10.0, _WINDOW_SECONDS - elapsed + 5)
            print(
                f"  Rate limit buffer reached ({self._usage_15min}/{self._limit_15min} "
                f"requests used) — sleeping {wait:.0f}s for window reset...",
                file=sys.stderr,
            )
            time.sleep(wait)
            self._window_start = time.monotonic()
            self._usage_15min = 0

        if self._usage_daily >= self._limit_daily - _RATE_LIMIT_BUFFER:
            raise RuntimeError(
                f"Daily rate limit nearly exhausted "
                f"({self._usage_daily}/{self._limit_daily}). Try again tomorrow."
            )

    def _get(self, url: str, params: dict[str, Any] | None = None) -> Any:
        """Make a rate-limit-aware GET request and return parsed JSON.

        Handles 429 reactively (sleeps Retry-After, then retries once) and
        proactively throttles before the 15-min window is exhausted.
        """
        for attempt in range(2):
            response = httpx.get(url, headers=self._auth_headers(), params=params)

            if response.status_code == 429:
                wait = int(response.headers.get("Retry-After", _WINDOW_SECONDS))
                print(
                    f"  429 received — sleeping {wait}s before retry...",
                    file=sys.stderr,
                )
                time.sleep(wait)
                self._window_start = time.monotonic()
                self._usage_15min = 0
                if attempt == 0:
                    continue
                response.raise_for_status()

            response.raise_for_status()
            self._update_usage(response)
            self._check_and_throttle()
            return response.json()

    # ------------------------------------------------------------------
    # Athlete
    # ------------------------------------------------------------------

    def fetch_athlete(self) -> dict[str, Any]:
        athlete = self._get(STRAVA_ATHLETE_URL)
        self._write_cache_json(
            "athlete.json",
            {
                "fetched_at": datetime.now(timezone.utc).isoformat(),
                "athlete": athlete,
            },
        )
        return athlete

    # ------------------------------------------------------------------
    # Activities (summary list)
    # ------------------------------------------------------------------

    def fetch_activities(
        self,
        *,
        max_pages: int | None = None,
        per_page: int = DEFAULT_PER_PAGE,
        after: int | None = None,
        before: int | None = None,
    ) -> list[dict[str, Any]]:
        """Fetch the authenticated athlete's activity list (all pages).

        Args:
            max_pages: Stop after this many pages (None = fetch all).
            per_page:  Results per page (max 200).
            after:     Only return activities after this Unix timestamp.
            before:    Only return activities before this Unix timestamp.
        """
        all_activities: list[dict[str, Any]] = []
        page = 1

        while True:
            params: dict[str, Any] = {"per_page": per_page, "page": page}
            if after is not None:
                params["after"] = after
            if before is not None:
                params["before"] = before

            batch: list[dict[str, Any]] = self._get(STRAVA_ACTIVITIES_URL, params)
            if not batch:
                break

            self._write_cache_json(
                Path("activities") / f"page-{page:04d}.json",
                {
                    "fetched_at": datetime.now(timezone.utc).isoformat(),
                    "page": page,
                    "params": params,
                    "raw_activities": batch,
                },
            )

            for data in batch:
                all_activities.append(_parse_activity(data))

            if max_pages is not None and page >= max_pages:
                break

            page += 1

        return all_activities

    # ------------------------------------------------------------------
    # Activity detail + laps
    # ------------------------------------------------------------------

    def fetch_activity_detail(self, activity_id: int) -> dict[str, Any]:
        """Fetch the full detail for a single activity (includes laps)."""
        url = STRAVA_ACTIVITY_URL.format(activity_id=activity_id)
        detail = self._get(url)
        self._write_cache_json(
            Path("activity_details") / f"{int(activity_id)}.json",
            {
                "fetched_at": datetime.now(timezone.utc).isoformat(),
                "activity_id": int(activity_id),
                **detail,
            },
        )
        return detail

    def fetch_laps(self, activity_id: int) -> list[dict[str, Any]]:
        """Fetch parsed lap records for a single activity."""
        detail = self.fetch_activity_detail(activity_id)
        return [_parse_lap(activity_id, lap) for lap in detail.get("laps", [])]

    def iter_laps_batched(
        self,
        activity_ids: list[int],
        batch_size: int = 80,
    ):
        """Yield (batch_laps, fetched_ids) tuples, batch_size activities at a time.

        Yields after every ``batch_size`` activities so the caller can persist
        partial results before continuing — this is the recommended way to
        populate DuckDB so progress is not lost if the run is interrupted or
        the daily budget runs out mid-fetch.

        The rate limiter handles throttling automatically; no manual sleep needed.
        """
        batch_laps: list[dict[str, Any]] = []
        fetched_ids: list[int] = []

        for i, activity_id in enumerate(activity_ids):
            print(
                f"  [{i + 1}/{len(activity_ids)}] fetching laps for {activity_id}...",
                file=sys.stderr,
            )
            try:
                batch_laps.extend(self.fetch_laps(activity_id))
                fetched_ids.append(activity_id)
            except RuntimeError:
                # Daily limit exhausted — yield what we have and re-raise
                if batch_laps:
                    yield batch_laps, fetched_ids
                raise
            except Exception as exc:
                print(f"  Warning: skipping {activity_id}: {exc}", file=sys.stderr)

            if len(fetched_ids) >= batch_size:
                yield batch_laps, fetched_ids
                batch_laps = []
                fetched_ids = []

        if batch_laps:
            yield batch_laps, fetched_ids


# ------------------------------------------------------------------
# Parsing helpers
# ------------------------------------------------------------------

def _parse_activity(data: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": data["id"],
        "name": data["name"],
        "start_date_local": data["start_date_local"],
        "type": data["type"],
        "distance": data["distance"],
        "moving_time": data["moving_time"],
        "elapsed_time": data["elapsed_time"],
        "total_elevation_gain": data["total_elevation_gain"],
        "end_latlng": data.get("end_latlng"),
        "max_speed": data.get("max_speed"),
        "average_speed": data.get("average_speed"),
        "average_heartrate": data.get("average_heartrate") if data.get("has_heartrate") else None,
        "max_heartrate": data.get("max_heartrate") if data.get("has_heartrate") else None,
        "summary_polyline": data.get("map", {}).get("summary_polyline"),
    }


def _parse_lap(activity_id: int, lap: dict[str, Any]) -> dict[str, Any]:
    return {
        "workout_id": activity_id,
        "average_cadence": lap.get("average_cadence"),
        "average_heartrate": lap.get("average_heartrate"),
        "average_speed": lap.get("average_speed"),
        "distance": lap.get("distance"),
        "lap_id": lap.get("id"),
        "lap_index": lap.get("lap_index"),
        "max_heartrate": lap.get("max_heartrate"),
        "max_speed": lap.get("max_speed"),
        "moving_time": lap.get("moving_time"),
        "split": lap.get("split"),
    }


# ------------------------------------------------------------------
# Script entry point
# ------------------------------------------------------------------

def main() -> None:
    missing = [
        var
        for var in ("STRAVA_CLIENT_ID", "STRAVA_CLIENT_SECRET", "STRAVA_REFRESH_TOKEN")
        if not os.environ.get(var)
    ]
    if missing:
        print(
            f"Error: missing required environment variable(s): {', '.join(missing)}",
            file=sys.stderr,
        )
        sys.exit(1)

    extractor = StravaExtractor()

    print("Fetching athlete profile...", file=sys.stderr)
    athlete = extractor.fetch_athlete()
    print(
        f"Authenticated as: {athlete.get('firstname')} {athlete.get('lastname')} "
        f"(id={athlete.get('id')})",
        file=sys.stderr,
    )

    print("Fetching activities...", file=sys.stderr)
    activities = extractor.fetch_activities()
    print(f"Fetched {len(activities)} activities.", file=sys.stderr)

    run_ids = [a["id"] for a in activities if a.get("type", "").lower() == "run"]
    print(f"Fetching laps for {len(run_ids)} run activities...", file=sys.stderr)
    all_laps: list[dict[str, Any]] = []
    for batch_laps, _ in extractor.iter_laps_batched(run_ids):
        all_laps.extend(batch_laps)
    print(f"Fetched {len(all_laps)} lap records.", file=sys.stderr)

    print(json.dumps({"activities": activities, "laps": all_laps}, indent=2))


if __name__ == "__main__":
    if __package__ in (None, ""):
        SRC_ROOT = Path(__file__).resolve().parents[1]
        if str(SRC_ROOT) not in sys.path:
            sys.path.insert(0, str(SRC_ROOT))

    main()
