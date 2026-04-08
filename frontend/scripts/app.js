const API_BASE_URL = "http://127.0.0.1:8200/api/v1";

const datasetGrid = document.querySelector("#dataset-grid");
const apiStatus = document.querySelector("#api-status");

function setStatus(text, state) {
  apiStatus.textContent = text;
  apiStatus.dataset.state = state;
}

function renderDatasetCard(dataset) {
  const pills = [
    `${dataset.record_count} records`,
    `${dataset.column_count} columns`,
    ...dataset.sample_columns.slice(0, 2),
  ]
    .map((t) => `<span class="pill">${t}</span>`)
    .join("");

  const article = document.createElement("article");
  article.className = "dataset-card";
  article.innerHTML = `
    <h3>${dataset.title}</h3>
    <p>${dataset.description}</p>
    <div class="dataset-meta">${pills}</div>
  `;
  datasetGrid.appendChild(article);
}

async function fetchJson(path) {
  const res = await fetch(`${API_BASE_URL}${path}`);
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
  return res.json();
}

async function load() {
  try {
    setStatus("Connecting…", "");
    const { datasets } = await fetchJson("/datasets");
    datasets.forEach(renderDatasetCard);
    setStatus("Connected", "success");
  } catch (err) {
    datasetGrid.innerHTML = `
      <div class="error-card">
        <h3>API unavailable</h3>
        <p>Start the server with <code>uvicorn api:app --reload --port 8200</code> and refresh.</p>
      </div>
    `;
    setStatus("Disconnected", "error");
  }
}

load();

// ── Dashboard ────────────────────────────────────────────────────────────────

const dashCharts = [];

function destroyDashCharts() {
  dashCharts.forEach((c) => c.destroy());
  dashCharts.length = 0;
}

function makeLineChart(canvasId, labels, datasets, yLabel) {
  const canvas = document.getElementById(canvasId);
  if (!canvas) return;
  const chart = new Chart(canvas, {
    type: "line",
    data: { labels, datasets },
    options: {
      animation: false,
      responsive: true,
      maintainAspectRatio: false,
      plugins: { legend: { display: datasets.length > 1 } },
      scales: {
        x: {
          ticks: { maxTicksLimit: 8, font: { size: 11 } },
          grid: { color: "rgba(0,0,0,0.05)" },
        },
        y: {
          title: { display: !!yLabel, text: yLabel, font: { size: 11 } },
          ticks: { font: { size: 11 } },
          grid: { color: "rgba(0,0,0,0.05)" },
        },
      },
    },
  });
  dashCharts.push(chart);
}

function fmtWeekLabel(isoDate) {
  return new Date(isoDate).toLocaleDateString(undefined, { month: "short", day: "numeric" });
}

async function loadDashboard() {
  try {
    const [stepsRes, hrRes, sleepRes, vo2Res, sportRes] = await Promise.all([
      fetchJson("/analytics/steps/daily?days=365"),
      fetchJson("/analytics/heart-rate/trends?days=365"),
      fetchJson("/analytics/sleep/weekly?weeks=52"),
      fetchJson("/analytics/vo2max/trends"),
      fetchJson("/analytics/activities/summary"),
    ]);

    destroyDashCharts();

    // Steps
    const stepsData = stepsRes.data;
    makeLineChart(
      "dash-steps",
      stepsData.map((r) => fmtWeekLabel(r.date)),
      [
        {
          label: "Steps",
          data: stepsData.map((r) => r.steps),
          borderColor: "#2563eb",
          backgroundColor: "#2563eb18",
          borderWidth: 1.5,
          pointRadius: 0,
          fill: true,
          tension: 0.3,
          spanGaps: true,
        },
        {
          label: "7-day avg",
          data: stepsData.map((r) => r.rolling_7d_avg),
          borderColor: "#93c5fd",
          borderWidth: 2,
          pointRadius: 0,
          fill: false,
          tension: 0.3,
          spanGaps: true,
          borderDash: [4, 3],
        },
      ],
      "steps"
    );

    // Resting HR
    const hrData = hrRes.data;
    makeLineChart(
      "dash-hr",
      hrData.map((r) => fmtWeekLabel(r.date)),
      [
        {
          label: "Resting HR",
          data: hrData.map((r) => r.resting_hr),
          borderColor: "#ef4444",
          backgroundColor: "#ef444418",
          borderWidth: 1.5,
          pointRadius: 0,
          fill: true,
          tension: 0.3,
          spanGaps: true,
        },
        {
          label: "7-day avg",
          data: hrData.map((r) => r.rolling_7d_avg),
          borderColor: "#fca5a5",
          borderWidth: 2,
          pointRadius: 0,
          fill: false,
          tension: 0.3,
          spanGaps: true,
          borderDash: [4, 3],
        },
      ],
      "bpm"
    );

    // Sleep
    const sleepData = [...sleepRes.data].reverse();
    makeLineChart(
      "dash-sleep",
      sleepData.map((r) => fmtWeekLabel(r.week_start)),
      [
        {
          label: "Avg hours",
          data: sleepData.map((r) => r.avg_sleep_hours),
          borderColor: "#9333ea",
          backgroundColor: "#9333ea18",
          borderWidth: 1.5,
          pointRadius: 2,
          fill: true,
          tension: 0.3,
          spanGaps: true,
          yAxisID: "y",
        },
        {
          label: "Sleep score",
          data: sleepData.map((r) => r.avg_sleep_score),
          borderColor: "#c084fc",
          borderWidth: 2,
          pointRadius: 0,
          fill: false,
          tension: 0.3,
          spanGaps: true,
          borderDash: [4, 3],
          yAxisID: "y1",
        },
      ],
    );
    // sleep chart needs dual axes — rebuild with custom options
    dashCharts[dashCharts.length - 1].destroy();
    dashCharts.pop();
    const sleepCanvas = document.getElementById("dash-sleep");
    if (sleepCanvas) {
      const c = new Chart(sleepCanvas, {
        type: "line",
        data: {
          labels: sleepData.map((r) => fmtWeekLabel(r.week_start)),
          datasets: [
            {
              label: "Avg hours",
              data: sleepData.map((r) => r.avg_sleep_hours),
              borderColor: "#9333ea",
              backgroundColor: "#9333ea18",
              borderWidth: 1.5,
              pointRadius: 2,
              fill: true,
              tension: 0.3,
              spanGaps: true,
              yAxisID: "y",
            },
            {
              label: "Score",
              data: sleepData.map((r) => r.avg_sleep_score),
              borderColor: "#c084fc",
              borderWidth: 2,
              pointRadius: 0,
              fill: false,
              tension: 0.3,
              spanGaps: true,
              borderDash: [4, 3],
              yAxisID: "y1",
            },
          ],
        },
        options: {
          animation: false,
          responsive: true,
          maintainAspectRatio: false,
          plugins: { legend: { display: true, labels: { font: { size: 11 } } } },
          scales: {
            x: { ticks: { maxTicksLimit: 8, font: { size: 11 } }, grid: { color: "rgba(0,0,0,0.05)" } },
            y: { position: "left", title: { display: true, text: "hours", font: { size: 11 } }, ticks: { font: { size: 11 } }, grid: { color: "rgba(0,0,0,0.05)" } },
            y1: { position: "right", title: { display: true, text: "score", font: { size: 11 } }, ticks: { font: { size: 11 } }, grid: { drawOnChartArea: false } },
          },
        },
      });
      dashCharts.push(c);
    }

    // VO2 Max — group by sport
    const vo2Data = vo2Res.data;
    const vo2Sports = [...new Set(vo2Data.map((r) => r.sport))];
    const vo2Colors = ["#16a34a", "#2563eb", "#ef4444", "#f59e0b", "#9333ea"];
    makeLineChart(
      "dash-vo2",
      vo2Data.filter((r) => r.sport === vo2Sports[0]).map((r) => fmtWeekLabel(r.date)),
      vo2Sports.map((sport, i) => ({
        label: sport,
        data: vo2Data.filter((r) => r.sport === sport).map((r) => r.vo2_max),
        borderColor: vo2Colors[i % vo2Colors.length],
        borderWidth: 2,
        pointRadius: 3,
        fill: false,
        tension: 0.3,
        spanGaps: true,
      })),
      "mL/kg/min"
    );

    // Sport summary table
    const sportWrap = document.getElementById("sport-summary-wrap");
    if (sportWrap && sportRes.data.length) {
      const rows = sportRes.data
        .map(
          (r) => `<tr>
            <td>${r.sport ?? "—"}</td>
            <td>${r.total_activities}</td>
            <td>${r.total_distance_km != null ? r.total_distance_km + " km" : "—"}</td>
            <td>${r.avg_distance_km != null ? r.avg_distance_km + " km" : "—"}</td>
            <td>${r.avg_heart_rate != null ? r.avg_heart_rate + " bpm" : "—"}</td>
            <td>${r.avg_duration_min != null ? r.avg_duration_min + " min" : "—"}</td>
            <td>${r.total_calories != null ? r.total_calories.toLocaleString() + " kcal" : "—"}</td>
          </tr>`
        )
        .join("");
      sportWrap.innerHTML = `
        <table class="sport-summary-table">
          <thead>
            <tr>
              <th>Sport</th>
              <th>Activities</th>
              <th>Total dist.</th>
              <th>Avg dist.</th>
              <th>Avg HR</th>
              <th>Avg duration</th>
              <th>Total calories</th>
            </tr>
          </thead>
          <tbody>${rows}</tbody>
        </table>
      `;
    }
  } catch (err) {
    console.error("Dashboard load failed:", err);
  }
}

loadDashboard();

// ── Activities viewer ─────────────────────────────────────────────────────────

const actState = {
  sessions: [],
  total: 0,
  page: 0,
  pageSize: 20,
  sortBy: "start_time",
  sortDir: "desc",
  activeCharts: [],
};

const filterSport = document.querySelector("#filter-sport");
const filterDateFrom = document.querySelector("#filter-date-from");
const filterDateTo = document.querySelector("#filter-date-to");
const loadBtn = document.querySelector("#load-activities-btn");
const tableWrap = document.querySelector("#activities-table-wrap");
const pagination = document.querySelector("#pagination");
const pageInfo = document.querySelector("#page-info");
const pagePrev = document.querySelector("#page-prev");
const pageNext = document.querySelector("#page-next");
const detailPanel = document.querySelector("#detail-panel");
const detailTitle = document.querySelector("#detail-title");
const statChips = document.querySelector("#stat-chips");
const detailClose = document.querySelector("#detail-close");

function fmtDistance(m) {
  if (m == null) return "—";
  return (m / 1000).toFixed(2) + " km";
}

function fmtDuration(s) {
  if (s == null) return "—";
  const h = Math.floor(s / 3600);
  const m = Math.floor((s % 3600) / 60);
  const sec = Math.round(s % 60);
  return h > 0
    ? `${h}h ${m}m ${sec}s`
    : `${m}m ${sec}s`;
}

function fmtDate(ts) {
  if (!ts) return "—";
  return new Date(ts).toLocaleDateString(undefined, {
    year: "numeric", month: "short", day: "numeric",
  });
}

function fmtTime(ts) {
  if (!ts) return "";
  return new Date(ts).toLocaleTimeString(undefined, {
    hour: "2-digit", minute: "2-digit",
  });
}

function fmtSpeed(ms) {
  if (ms == null) return "—";
  return (ms * 3.6).toFixed(1) + " km/h";
}

function capitalize(str) {
  if (!str) return "—";
  return str.charAt(0).toUpperCase() + str.slice(1).toLowerCase();
}

async function loadActivities() {
  const params = new URLSearchParams({
    sort_by: actState.sortBy,
    sort_dir: actState.sortDir,
    limit: actState.pageSize,
    offset: actState.page * actState.pageSize,
  });

  const sport = filterSport.value;
  const dateFrom = filterDateFrom.value;
  const dateTo = filterDateTo.value;
  if (sport) params.set("sport", sport);
  if (dateFrom) params.set("date_from", dateFrom);
  if (dateTo) params.set("date_to", dateTo);

  tableWrap.innerHTML = `<p class="load-prompt">Loading…</p>`;
  pagination.classList.add("hidden");

  try {
    const data = await fetchJson(`/activities?${params}`);
    actState.sessions = data.sessions;
    actState.total = data.total;
    renderTable();
  } catch {
    tableWrap.innerHTML = `<p class="load-prompt error-text">Failed to load activities. Is the API running?</p>`;
  }
}

function renderTable() {
  if (!actState.sessions.length) {
    tableWrap.innerHTML = `<p class="load-prompt">No activities match your filters.</p>`;
    pagination.classList.add("hidden");
    return;
  }

  const cols = [
    { key: "sport",              label: "Sport" },
    { key: "start_time",         label: "Date" },
    { key: "total_distance",     label: "Distance" },
    { key: "total_elapsed_time", label: "Duration" },
    { key: "total_calories",     label: "Calories" },
    { key: "avg_heart_rate",     label: "Avg HR" },
    { key: "max_heart_rate",     label: "Max HR" },
    { key: "avg_speed",          label: "Avg Speed" },
    { key: "avg_cadence",        label: "Cadence" },
    { key: "total_ascent",       label: "Ascent" },
  ];

  const headers = cols.map(({ key, label }) => {
    const active = actState.sortBy === key;
    const arrow = active ? (actState.sortDir === "asc" ? " ↑" : " ↓") : "";
    return `<th class="sortable-header${active ? " sort-active" : ""}" data-key="${key}">${label}${arrow}</th>`;
  }).join("");

  const rows = actState.sessions.map((s) => {
    const cells = cols.map(({ key }) => {
      let val;
      if (key === "sport") val = capitalize(s.sport);
      else if (key === "start_time") val = `${fmtDate(s.start_time)} <span class="time-dim">${fmtTime(s.start_time)}</span>`;
      else if (key === "total_distance") val = fmtDistance(s.total_distance);
      else if (key === "total_elapsed_time") val = fmtDuration(s.total_elapsed_time);
      else if (key === "total_calories") val = s.total_calories != null ? s.total_calories + " kcal" : "—";
      else if (key === "avg_heart_rate") val = s.avg_heart_rate != null ? s.avg_heart_rate + " bpm" : "—";
      else if (key === "max_heart_rate") val = s.max_heart_rate != null ? s.max_heart_rate + " bpm" : "—";
      else if (key === "avg_speed") val = fmtSpeed(s.avg_speed);
      else if (key === "avg_cadence") val = s.avg_cadence != null ? s.avg_cadence + " rpm" : "—";
      else if (key === "total_ascent") val = s.total_ascent != null ? s.total_ascent + " m" : "—";
      else val = s[key] ?? "—";
      return `<td>${val}</td>`;
    }).join("");
    return `<tr class="activity-row" data-id="${s.activity_id}">${cells}</tr>`;
  }).join("");

  tableWrap.innerHTML = `
    <div class="preview-table-wrap">
      <table class="preview-table activities-table">
        <thead><tr>${headers}</tr></thead>
        <tbody>${rows}</tbody>
      </table>
    </div>
  `;

  // Sort header clicks
  tableWrap.querySelectorAll(".sortable-header").forEach((th) => {
    th.addEventListener("click", () => {
      const key = th.dataset.key;
      if (actState.sortBy === key) {
        actState.sortDir = actState.sortDir === "asc" ? "desc" : "asc";
      } else {
        actState.sortBy = key;
        actState.sortDir = "desc";
      }
      actState.page = 0;
      loadActivities();
    });
  });

  // Row click → detail
  tableWrap.querySelectorAll(".activity-row").forEach((row) => {
    row.addEventListener("click", () => openDetail(row.dataset.id));
  });

  // Pagination
  const totalPages = Math.ceil(actState.total / actState.pageSize);
  if (totalPages > 1) {
    pagination.classList.remove("hidden");
    pageInfo.textContent = `Page ${actState.page + 1} of ${totalPages} (${actState.total} total)`;
    pagePrev.disabled = actState.page === 0;
    pageNext.disabled = actState.page >= totalPages - 1;
  } else {
    pagination.classList.add("hidden");
  }
}

pagePrev.addEventListener("click", () => {
  if (actState.page > 0) { actState.page--; loadActivities(); }
});
pageNext.addEventListener("click", () => {
  actState.page++;
  loadActivities();
});

async function openDetail(activityId) {
  detailPanel.classList.remove("hidden");
  detailTitle.textContent = "Loading…";
  statChips.innerHTML = "";
  destroyActivityCharts();

  detailPanel.scrollIntoView({ behavior: "smooth", block: "start" });

  try {
    const [{ session }, { records }] = await Promise.all([
      fetchJson(`/activities/${activityId}`),
      fetchJson(`/activities/${activityId}/records`),
    ]);

    detailTitle.textContent = `${capitalize(session.sport)} — ${fmtDate(session.start_time)}`;

    const chips = [
      ["Distance",  fmtDistance(session.total_distance)],
      ["Duration",  fmtDuration(session.total_elapsed_time)],
      ["Calories",  session.total_calories != null ? session.total_calories + " kcal" : "—"],
      ["Avg HR",    session.avg_heart_rate != null ? session.avg_heart_rate + " bpm" : "—"],
      ["Max HR",    session.max_heart_rate != null ? session.max_heart_rate + " bpm" : "—"],
      ["Avg Speed", fmtSpeed(session.avg_speed)],
      ["Cadence",   session.avg_cadence != null ? session.avg_cadence + " rpm" : "—"],
      ["Ascent",    session.total_ascent != null ? session.total_ascent + " m" : "—"],
      ["Descent",   session.total_descent != null ? session.total_descent + " m" : "—"],
    ];
    statChips.innerHTML = chips
      .map(([label, val]) => `<div class="stat-chip"><span class="stat-chip-label">${label}</span><span class="stat-chip-val">${val}</span></div>`)
      .join("");

    renderActivityCharts(records);
  } catch {
    detailTitle.textContent = "Failed to load activity detail.";
  }
}

function closeDetail() {
  detailPanel.classList.add("hidden");
  destroyActivityCharts();
}

function destroyActivityCharts() {
  actState.activeCharts.forEach((c) => c.destroy());
  actState.activeCharts = [];
}

function renderActivityCharts(records) {
  if (!records.length) return;

  // Sample down to at most 500 points to keep Chart.js responsive
  const step = Math.max(1, Math.floor(records.length / 500));
  const sampled = records.filter((_, i) => i % step === 0);

  const labels = sampled.map((r) => {
    if (!r.timestamp) return "";
    const d = new Date(r.timestamp);
    return d.toLocaleTimeString(undefined, { hour: "2-digit", minute: "2-digit" });
  });

  const chartDefs = [
    { id: "chart-hr",       field: "heart_rate", color: "#ef4444" },
    { id: "chart-speed",    field: "speed",      color: "#2563eb" },
    { id: "chart-cadence",  field: "cadence",    color: "#16a34a" },
    { id: "chart-altitude", field: "altitude",   color: "#9333ea" },
  ];

  chartDefs.forEach(({ id, field, color }) => {
    const canvas = document.getElementById(id);
    if (!canvas) return;
    const data = sampled.map((r) => r[field] ?? null);
    const hasData = data.some((v) => v !== null);
    if (!hasData) {
      canvas.parentElement.style.opacity = "0.35";
      return;
    }
    canvas.parentElement.style.opacity = "1";
    const chart = new Chart(canvas, {
      type: "line",
      data: {
        labels,
        datasets: [{
          data,
          borderColor: color,
          backgroundColor: color + "18",
          borderWidth: 1.5,
          pointRadius: 0,
          fill: true,
          tension: 0.3,
          spanGaps: true,
        }],
      },
      options: {
        animation: false,
        responsive: true,
        maintainAspectRatio: false,
        plugins: { legend: { display: false } },
        scales: {
          x: {
            ticks: { maxTicksLimit: 8, font: { size: 11 } },
            grid: { color: "rgba(0,0,0,0.05)" },
          },
          y: {
            ticks: { font: { size: 11 } },
            grid: { color: "rgba(0,0,0,0.05)" },
          },
        },
      },
    });
    actState.activeCharts.push(chart);
  });
}

detailClose.addEventListener("click", closeDetail);
loadBtn.addEventListener("click", () => { actState.page = 0; loadActivities(); });
