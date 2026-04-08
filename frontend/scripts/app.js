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

async function fetchJson(path, options = {}) {
  const res = await fetch(`${API_BASE_URL}${path}`, options);
  if (!res.ok) {
    let detail = `HTTP ${res.status}`;
    try {
      const payload = await res.json();
      if (payload && payload.detail) detail = payload.detail;
    } catch {
      // Ignore parse failures and fall back to status code.
    }
    throw new Error(detail);
  }
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
            <td>${r.total_distance_km != null ? (r.total_distance_km * 0.621371).toFixed(1) + " mi" : "—"}</td>
            <td>${r.avg_distance_km != null ? (r.avg_distance_km * 0.621371).toFixed(2) + " mi" : "—"}</td>
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

// ── Training plans ───────────────────────────────────────────────────────────

const upcomingPlanWrap = document.querySelector("#upcoming-plan-wrap");
const upcomingPlanSummary = document.querySelector("#upcoming-plan-summary");
const planStatus = document.querySelector("#plan-status");
const planBuilderForm = document.querySelector("#plan-builder-form");
const planRaceType = document.querySelector("#plan-race-type");
const planRaceDate = document.querySelector("#plan-race-date");
const planGoalTime = document.querySelector("#plan-goal-time");
const planEventNameOrDistance = document.querySelector("#plan-event-name-or-distance");
const planAreaOfEmphasis = document.querySelector("#plan-area-of-emphasis");
const planInjuryHistory = document.querySelector("#plan-injury-history");
const planEquipment = document.querySelector("#plan-equipment");
const planOtherThoughts = document.querySelector("#plan-other-thoughts");
const planIncludeStrength = document.querySelector("#plan-include-strength");
const planIncludeMobility = document.querySelector("#plan-include-mobility");
const triathlonFields = document.querySelector("#triathlon-fields");
const planTriathlonNotes = document.querySelector("#plan-triathlon-notes");
const planOutput = document.querySelector("#plan-output");
const planFormMessage = document.querySelector("#plan-form-message");
const planGenerateBtn = document.querySelector("#plan-generate-btn");
const canEditTrainingPlan = Boolean(planBuilderForm);

function setPlanStatus(text, state) {
  if (!planStatus) return;
  planStatus.textContent = text;
  planStatus.dataset.state = state;
}

function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function collectCheckedValues(containerId) {
  return Array.from(document.querySelectorAll(`#${containerId} input:checked`)).map((input) => input.value);
}

function setTriathlonFieldsVisibility() {
  if (!triathlonFields || !planRaceType) return;
  triathlonFields.classList.toggle("hidden", planRaceType.value !== "triathlon");
}

function fmtWorkoutSubtitle(workout) {
  if (workout.is_rest_day) return "Rest day";
  const parts = [];
  if (workout.distance_miles != null && workout.distance_miles > 0) {
    parts.push(`${Number(workout.distance_miles).toFixed(1)} mi`);
  }
  if (workout.duration_minutes != null && workout.duration_minutes > 0) {
    parts.push(`${workout.duration_minutes} min`);
  }
  if (workout.intensity) {
    parts.push(capitalize(workout.intensity));
  }
  return parts.join(" • ") || "Planned session";
}

function serializeWorkoutField(value) {
  return value == null ? "" : escapeHtml(String(value));
}

function renderWorkoutEditForm(workout) {
  if (!canEditTrainingPlan) return "";
  return `
    <div class="plan-workout-actions">
      <button type="button" class="btn btn-secondary plan-workout-edit-btn" data-workout-edit="${escapeHtml(workout.workout_id)}">Edit workout</button>
    </div>
    <form class="plan-workout-edit-form hidden" data-workout-form="${escapeHtml(workout.workout_id)}">
      <div class="plan-form-grid">
        <label class="form-field">
          <span>Date</span>
          <input type="date" name="workout_date" value="${serializeWorkoutField(workout.workout_date)}" required />
        </label>
        <label class="form-field">
          <span>Discipline</span>
          <input type="text" name="discipline" value="${serializeWorkoutField(workout.discipline)}" required />
        </label>
        <label class="form-field">
          <span>Title</span>
          <input type="text" name="title" value="${serializeWorkoutField(workout.title)}" required />
        </label>
        <label class="form-field">
          <span>Intensity</span>
          <input type="text" name="intensity" value="${serializeWorkoutField(workout.intensity)}" />
        </label>
        <label class="form-field">
          <span>Duration Minutes</span>
          <input type="number" name="duration_minutes" min="0" step="1" value="${serializeWorkoutField(workout.duration_minutes)}" />
        </label>
        <label class="form-field">
          <span>Distance Miles</span>
          <input type="number" name="distance_miles" min="0" step="0.1" value="${serializeWorkoutField(workout.distance_miles)}" />
        </label>
        <label class="form-field form-field--full">
          <span>Description</span>
          <textarea name="description" rows="3">${serializeWorkoutField(workout.description)}</textarea>
        </label>
        <label class="form-field form-field--full">
          <span>Mobility Notes</span>
          <textarea name="mobility_notes" rows="2">${serializeWorkoutField(workout.mobility_notes)}</textarea>
        </label>
        <label class="form-field form-field--full">
          <span>Strength Notes</span>
          <textarea name="strength_notes" rows="2">${serializeWorkoutField(workout.strength_notes)}</textarea>
        </label>
        <label class="form-field form-field--full">
          <span>Recovery Notes</span>
          <textarea name="injury_notes" rows="2">${serializeWorkoutField(workout.injury_notes)}</textarea>
        </label>
      </div>
      <div class="plan-form-row">
        <label class="checkbox-field">
          <input type="checkbox" name="is_rest_day" ${workout.is_rest_day ? "checked" : ""} />
          <span>Rest day</span>
        </label>
        <label class="checkbox-field">
          <input type="checkbox" name="is_cross_training" ${workout.is_cross_training ? "checked" : ""} />
          <span>Cross-training</span>
        </label>
      </div>
      <div class="plan-form-actions">
        <button type="submit" class="btn btn-primary">Save changes</button>
        <button type="button" class="btn btn-secondary" data-workout-cancel="${escapeHtml(workout.workout_id)}">Cancel</button>
        <p class="form-inline-message" data-workout-message></p>
      </div>
    </form>
  `;
}

function parseOptionalNumber(value, parser) {
  if (value == null || value === "") return null;
  const parsed = parser(value);
  return Number.isNaN(parsed) ? null : parsed;
}

function buildWorkoutUpdatePayload(form) {
  const formData = new FormData(form);
  return {
    workout_date: String(formData.get("workout_date") || ""),
    discipline: String(formData.get("discipline") || "").trim(),
    title: String(formData.get("title") || "").trim(),
    description: String(formData.get("description") || "").trim(),
    duration_minutes: parseOptionalNumber(formData.get("duration_minutes"), Number.parseInt),
    distance_miles: parseOptionalNumber(formData.get("distance_miles"), Number.parseFloat),
    intensity: String(formData.get("intensity") || "").trim(),
    is_rest_day: formData.get("is_rest_day") === "on",
    is_cross_training: formData.get("is_cross_training") === "on",
    mobility_notes: String(formData.get("mobility_notes") || "").trim(),
    strength_notes: String(formData.get("strength_notes") || "").trim(),
    injury_notes: String(formData.get("injury_notes") || "").trim(),
  };
}

function hideWorkoutEditForms(exceptWorkoutId = "") {
  if (!planOutput) return;
  planOutput.querySelectorAll(".plan-workout-edit-form").forEach((form) => {
    if (form.dataset.workoutForm !== exceptWorkoutId) {
      form.classList.add("hidden");
    }
  });
}

function renderUpcomingPlan(payload) {
  if (!upcomingPlanWrap || !upcomingPlanSummary) return;
  if (!payload.plan) {
    setPlanStatus("No active plan", "");
    upcomingPlanSummary.textContent = "Generate a plan to see the next 7 days here.";
    upcomingPlanWrap.innerHTML = `<p class="load-prompt">No active training plan yet.</p>`;
    return;
  }

  setPlanStatus("Active plan", "success");
  upcomingPlanSummary.textContent = `${payload.plan.plan_title} • Race day ${fmtDate(payload.plan.race_date)}`;

  if (!payload.days || payload.days.length === 0) {
    upcomingPlanWrap.innerHTML = `<p class="load-prompt">No planned workouts in the next 7 days.</p>`;
    return;
  }

  const items = payload.days.map((day) => `
    <article class="upcoming-workout">
      <div>
        <p class="upcoming-workout-day">${escapeHtml(day.day_name)} • ${escapeHtml(fmtDate(day.workout_date))}</p>
        <h4>${escapeHtml(day.title)}</h4>
        <p class="upcoming-workout-meta">${escapeHtml(capitalize(day.discipline))} • ${escapeHtml(fmtWorkoutSubtitle(day))}</p>
        <p class="upcoming-workout-desc">${escapeHtml(day.description)}</p>
      </div>
    </article>
  `).join("");
  upcomingPlanWrap.innerHTML = `<div class="upcoming-workout-list">${items}</div>`;
}

function renderTrainingPlan(payload) {
  if (!planOutput) return;
  if (!payload.plan) {
    planOutput.innerHTML = `<p class="load-prompt">No active plan yet.</p>`;
    return;
  }

  const intro = `
    <div class="plan-output-header">
      <h3>${escapeHtml(payload.plan.plan_title)}</h3>
      <p>${escapeHtml(payload.plan.overview || "")}</p>
      <div class="dataset-meta">
        <span class="pill">${escapeHtml(capitalize(payload.plan.race_type))}</span>
        <span class="pill">${escapeHtml(payload.plan.event_name_or_distance)}</span>
        <span class="pill">Race day ${escapeHtml(fmtDate(payload.plan.race_date))}</span>
        ${payload.plan.goal_time ? `<span class="pill">Goal ${escapeHtml(payload.plan.goal_time)}</span>` : ""}
      </div>
    </div>
  `;

  const weeks = payload.weeks.map((week) => {
    const workouts = week.workouts.map((workout) => `
      <li class="plan-workout">
        <div class="plan-workout-top">
          <strong>${escapeHtml(fmtDate(workout.workout_date))}</strong>
          <span>${escapeHtml(capitalize(workout.discipline))}</span>
        </div>
        <p class="plan-workout-title">${escapeHtml(workout.title)}</p>
        <p class="plan-workout-meta">${escapeHtml(fmtWorkoutSubtitle(workout))}</p>
        <p class="plan-workout-desc">${escapeHtml(workout.description)}</p>
        ${workout.mobility_notes ? `<p class="plan-workout-note">Mobility: ${escapeHtml(workout.mobility_notes)}</p>` : ""}
        ${workout.strength_notes ? `<p class="plan-workout-note">Strength: ${escapeHtml(workout.strength_notes)}</p>` : ""}
        ${workout.injury_notes ? `<p class="plan-workout-note">Recovery: ${escapeHtml(workout.injury_notes)}</p>` : ""}
        ${renderWorkoutEditForm(workout)}
      </li>
    `).join("");

    return `
      <section class="plan-week">
        <div class="plan-week-header">
          <div>
            <h4>Week ${week.week_number}</h4>
            <p>${escapeHtml(fmtDate(week.week_start))} to ${escapeHtml(fmtDate(week.week_end))}</p>
          </div>
          <div class="plan-week-focus">
            <strong>${escapeHtml(week.focus)}</strong>
            <p>${escapeHtml(week.summary)}</p>
          </div>
        </div>
        <ul class="plan-workout-list">${workouts}</ul>
      </section>
    `;
  }).join("");

  planOutput.innerHTML = `${intro}<div class="plan-week-list">${weeks}</div>`;
}

async function loadTrainingPlan() {
  try {
    setPlanStatus("Loading…", "");
    const requests = [fetchJson("/training-plans/active")];
    if (upcomingPlanWrap) requests.push(fetchJson("/training-plans/upcoming?days=7"));
    const [activePlan, upcomingPlan] = await Promise.all(requests);
    renderTrainingPlan(activePlan);
    if (upcomingPlan) renderUpcomingPlan(upcomingPlan);
  } catch (err) {
    setPlanStatus("Plan error", "error");
    if (upcomingPlanSummary) upcomingPlanSummary.textContent = "Could not load the training plan.";
    if (upcomingPlanWrap) upcomingPlanWrap.innerHTML = `<p class="load-prompt error-text">${escapeHtml(err.message)}</p>`;
    if (planOutput) planOutput.innerHTML = `<p class="load-prompt error-text">${escapeHtml(err.message)}</p>`;
  }
}

function buildTrainingPlanPayload() {
  if (!planRaceType || !planRaceDate || !planEventNameOrDistance) return null;
  return {
    race_type: planRaceType.value,
    race_date: planRaceDate.value,
    goal_time: planGoalTime.value.trim() || null,
    event_name_or_distance: planEventNameOrDistance.value.trim(),
    area_of_emphasis: planAreaOfEmphasis ? planAreaOfEmphasis.value.trim() : "",
    injury_history: planInjuryHistory.value.trim(),
    other_thoughts: planOtherThoughts ? planOtherThoughts.value.trim() : "",
    include_strength: planIncludeStrength.checked,
    include_mobility: planIncludeMobility.checked,
    equipment: planEquipment.value.trim(),
    preferred_days: collectCheckedValues("preferred-day-options"),
    blocked_days: collectCheckedValues("blocked-day-options"),
    triathlon_disciplines: collectCheckedValues("triathlon-discipline-options"),
    triathlon_notes: planTriathlonNotes.value.trim(),
  };
}

async function handleTrainingPlanSubmit(event) {
  event.preventDefault();
  if (planFormMessage) planFormMessage.textContent = "";

  const payload = buildTrainingPlanPayload();
  if (!payload) return;
  if (!payload.race_date || !payload.event_name_or_distance) {
    if (planFormMessage) planFormMessage.textContent = "Race day and event are required.";
    return;
  }

  if (planGenerateBtn) {
    planGenerateBtn.disabled = true;
    planGenerateBtn.textContent = "Generating…";
  }
  if (planOutput) planOutput.innerHTML = `<p class="load-prompt">Generating plan from your history…</p>`;

  try {
    const plan = await fetchJson("/training-plans/generate", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify(payload),
    });
    if (planFormMessage) planFormMessage.textContent = "Plan generated.";
    renderTrainingPlan(plan);
    if (upcomingPlanWrap) {
      renderUpcomingPlan(await fetchJson("/training-plans/upcoming?days=7"));
    }
  } catch (err) {
    if (planFormMessage) planFormMessage.textContent = err.message;
    if (planOutput) planOutput.innerHTML = `<p class="load-prompt error-text">${escapeHtml(err.message)}</p>`;
  } finally {
    if (planGenerateBtn) {
      planGenerateBtn.disabled = false;
      planGenerateBtn.textContent = "Generate plan";
    }
  }
}

async function handlePlanOutputClick(event) {
  const editButton = event.target.closest("[data-workout-edit]");
  if (editButton) {
    const workoutId = editButton.dataset.workoutEdit;
    hideWorkoutEditForms(workoutId);
    const form = planOutput ? planOutput.querySelector(`[data-workout-form="${workoutId}"]`) : null;
    if (form) {
      form.classList.toggle("hidden");
    }
    return;
  }

  const cancelButton = event.target.closest("[data-workout-cancel]");
  if (cancelButton) {
    const workoutId = cancelButton.dataset.workoutCancel;
    const form = planOutput ? planOutput.querySelector(`[data-workout-form="${workoutId}"]`) : null;
    if (form) {
      form.classList.add("hidden");
    }
  }
}

async function handleWorkoutEditSubmit(event) {
  const form = event.target.closest(".plan-workout-edit-form");
  if (!form) return;

  event.preventDefault();
  const workoutId = form.dataset.workoutForm;
  const message = form.querySelector("[data-workout-message]");
  const submitButton = form.querySelector('button[type="submit"]');
  const payload = buildWorkoutUpdatePayload(form);

  if (message) message.textContent = "";
  if (submitButton) {
    submitButton.disabled = true;
    submitButton.textContent = "Saving…";
  }

  try {
    const plan = await fetchJson(`/training-plans/workouts/${encodeURIComponent(workoutId)}`, {
      method: "PUT",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify(payload),
    });
    renderTrainingPlan(plan);
    if (message) message.textContent = "Workout updated.";
    if (planFormMessage) planFormMessage.textContent = "Workout updated.";
    if (upcomingPlanWrap) {
      renderUpcomingPlan(await fetchJson("/training-plans/upcoming?days=7"));
    }
  } catch (err) {
    if (message) message.textContent = err.message;
  } finally {
    if (submitButton) {
      submitButton.disabled = false;
      submitButton.textContent = "Save changes";
    }
  }
}

if (planRaceType) {
  planRaceType.addEventListener("change", setTriathlonFieldsVisibility);
  setTriathlonFieldsVisibility();
}
if (planBuilderForm) {
  planBuilderForm.addEventListener("submit", handleTrainingPlanSubmit);
}
if (planOutput && canEditTrainingPlan) {
  planOutput.addEventListener("click", handlePlanOutputClick);
  planOutput.addEventListener("submit", handleWorkoutEditSubmit);
}
if (planOutput || upcomingPlanWrap) {
  loadTrainingPlan();
}

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
  return (m / 1609.344).toFixed(2) + " mi";
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
  const value = /^\d{4}-\d{2}-\d{2}$/.test(ts) ? `${ts}T00:00:00` : ts;
  return new Date(value).toLocaleDateString(undefined, {
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
  return (ms * 2.23694).toFixed(1) + " mph";
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

// ── Strava ────────────────────────────────────────────────────────────────────

const stravaViewMonths     = document.querySelector("#strava-view-months");
const stravaViewActivities = document.querySelector("#strava-view-activities");
const stravaViewLaps       = document.querySelector("#strava-view-laps");
const stravaMonthGrid      = document.querySelector("#strava-month-grid");
const stravaActivityList   = document.querySelector("#strava-activity-list");
const stravaMonthTitle     = document.querySelector("#strava-month-title");
const stravaActivityTitle  = document.querySelector("#strava-activity-title");
const stravaActivityChips  = document.querySelector("#strava-activity-chips");
const stravaLapsWrap       = document.querySelector("#strava-laps-wrap");
const stravaBackMonths     = document.querySelector("#strava-back-months");
const stravaBackActivities = document.querySelector("#strava-back-activities");

const MONTH_NAMES = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"];

// Strava nav state so the back buttons know where to return
let stravaCurrentYear  = null;
let stravaCurrentMonth = null;

function fmtPace(avgSpeedMs) {
  if (!avgSpeedMs || avgSpeedMs === 0) return "—";
  const minPerMi = 1609.344 / (avgSpeedMs * 60);
  const mins = Math.floor(minPerMi);
  const secs = Math.round((minPerMi - mins) * 60);
  return `${mins}:${secs.toString().padStart(2, "0")} /mi`;
}

function showStravaView(view) {
  stravaViewMonths.classList.add("hidden");
  stravaViewActivities.classList.add("hidden");
  stravaViewLaps.classList.add("hidden");
  view.classList.remove("hidden");
  view.scrollIntoView({ behavior: "smooth", block: "start" });
}

// ── View 1: month index ──────────────────────────────────────────────────────

async function loadStravaMonths() {
  try {
    const { months } = await fetchJson("/strava/months");

    if (!months || months.length === 0) {
      stravaMonthGrid.innerHTML = `<p class="load-prompt">No Strava data found. Run <code>python db/build.py --table strava</code> to import.</p>`;
      return;
    }

    stravaMonthGrid.innerHTML = "";
    months.forEach((m) => {
      const label = `${MONTH_NAMES[m.month - 1]} ${m.year}`;
      const card = document.createElement("article");
      card.className = "strava-month-card";
      card.innerHTML = `
        <span class="strava-month-label">${label}</span>
        <span class="strava-month-count">${m.activity_count} ${m.activity_count === 1 ? "activity" : "activities"}</span>
        <div class="strava-month-stats">
          <span>${m.total_distance_mi != null ? m.total_distance_mi + " mi" : "—"}</span>
          <span>${fmtDuration(m.total_moving_time_s)}</span>
        </div>
      `;
      card.addEventListener("click", () => loadStravaActivitiesForMonth(m.year, m.month, label));
      stravaMonthGrid.appendChild(card);
    });
  } catch {
    stravaMonthGrid.innerHTML = `<p class="load-prompt error-text">Could not load Strava months. Is the API running?</p>`;
  }
}

// ── View 2: activities for a month ───────────────────────────────────────────

async function loadStravaActivitiesForMonth(year, month, label) {
  stravaCurrentYear  = year;
  stravaCurrentMonth = month;
  stravaMonthTitle.textContent = label;
  stravaActivityList.innerHTML = `<p class="load-prompt">Loading…</p>`;
  showStravaView(stravaViewActivities);

  try {
    const { activities } = await fetchJson(`/strava/activities?year=${year}&month=${month}&limit=200`);

    if (!activities || activities.length === 0) {
      stravaActivityList.innerHTML = `<p class="load-prompt">No activities for ${label}.</p>`;
      return;
    }

    const rows = activities.map((a) => {
      const isRun = (a.type || "").toLowerCase() === "run";
      const pace  = isRun ? fmtPace(a.average_speed) : fmtSpeed(a.average_speed);
      const date  = a.start_date_local
        ? new Date(a.start_date_local).toLocaleDateString(undefined, { weekday: "short", month: "short", day: "numeric" })
        : "—";
      return `
        <tr class="strava-activity-row" data-id="${a.id}" data-name="${(a.name || "Untitled").replace(/"/g, "&quot;")}">
          <td>${date}</td>
          <td class="strava-activity-name-cell">${a.name || "Untitled"}</td>
          <td>${capitalize(a.type || "")}</td>
          <td>${fmtDistance(a.distance)}</td>
          <td>${fmtDuration(a.moving_time)}</td>
          <td>${pace}</td>
          <td>${a.average_heartrate != null ? a.average_heartrate + " bpm" : "—"}</td>
          <td>${a.total_elevation_gain != null ? a.total_elevation_gain + " m" : "—"}</td>
        </tr>`;
    }).join("");

    stravaActivityList.innerHTML = `
      <div class="preview-table-wrap">
        <table class="preview-table strava-activities-table">
          <thead>
            <tr>
              <th>Date</th><th>Name</th><th>Type</th><th>Distance</th>
              <th>Duration</th><th>Pace / Speed</th><th>Avg HR</th><th>Elevation</th>
            </tr>
          </thead>
          <tbody>${rows}</tbody>
        </table>
      </div>`;

    stravaActivityList.querySelectorAll(".strava-activity-row").forEach((row) => {
      row.addEventListener("click", () =>
        loadStravaLaps(parseInt(row.dataset.id), row.dataset.name, activities.find((a) => a.id == row.dataset.id))
      );
    });
  } catch {
    stravaActivityList.innerHTML = `<p class="load-prompt error-text">Failed to load activities.</p>`;
  }
}

// ── View 3: laps for an activity ─────────────────────────────────────────────

async function loadStravaLaps(activityId, activityName, activity) {
  stravaActivityTitle.textContent = activityName;
  stravaActivityChips.innerHTML   = "";
  stravaLapsWrap.innerHTML        = `<p class="load-prompt">Loading splits…</p>`;
  showStravaView(stravaViewLaps);

  // Summary chips from the activity row
  if (activity) {
    const isRun = (activity.type || "").toLowerCase() === "run";
    const chips = [
      ["Date",     activity.start_date_local ? new Date(activity.start_date_local).toLocaleDateString(undefined, { month: "short", day: "numeric", year: "numeric" }) : "—"],
      ["Distance", fmtDistance(activity.distance)],
      ["Duration", fmtDuration(activity.moving_time)],
      [isRun ? "Pace" : "Speed", isRun ? fmtPace(activity.average_speed) : fmtSpeed(activity.average_speed)],
      ["Avg HR",   activity.average_heartrate != null ? activity.average_heartrate + " bpm" : "—"],
      ["Max HR",   activity.max_heartrate     != null ? activity.max_heartrate     + " bpm" : "—"],
      ["Elevation",activity.total_elevation_gain != null ? activity.total_elevation_gain + " m" : "—"],
    ];
    stravaActivityChips.innerHTML = chips
      .map(([l, v]) => `<div class="stat-chip"><span class="stat-chip-label">${l}</span><span class="stat-chip-val">${v}</span></div>`)
      .join("");
  }

  try {
    const { laps } = await fetchJson(`/strava/activities/${activityId}/laps`);

    if (!laps || laps.length === 0) {
      stravaLapsWrap.innerHTML = `<p class="load-prompt">No split data available for this activity.</p>`;
      return;
    }

    const rows = laps.map((lap) => {
      const isRun = activity && (activity.type || "").toLowerCase() === "run";
      const pace  = isRun ? fmtPace(lap.average_speed) : fmtSpeed(lap.average_speed);
      return `
        <tr>
          <td>${lap.lap_index != null ? lap.lap_index : "—"}</td>
          <td>${fmtDistance(lap.distance)}</td>
          <td>${fmtDuration(lap.moving_time)}</td>
          <td>${pace}</td>
          <td>${lap.average_heartrate != null ? lap.average_heartrate + " bpm" : "—"}</td>
          <td>${lap.max_heartrate     != null ? lap.max_heartrate     + " bpm" : "—"}</td>
          <td>${lap.average_cadence   != null ? Math.round(lap.average_cadence) + " rpm" : "—"}</td>
        </tr>`;
    }).join("");

    stravaLapsWrap.innerHTML = `
      <div class="preview-table-wrap">
        <table class="preview-table strava-laps-table">
          <thead>
            <tr>
              <th>Lap</th><th>Distance</th><th>Time</th><th>Pace / Speed</th>
              <th>Avg HR</th><th>Max HR</th><th>Cadence</th>
            </tr>
          </thead>
          <tbody>${rows}</tbody>
        </table>
      </div>`;
  } catch {
    stravaLapsWrap.innerHTML = `<p class="load-prompt error-text">Failed to load splits.</p>`;
  }
}

// ── Activity Calendar ─────────────────────────────────────────────────────────

const calendarYearSelect  = document.querySelector("#calendar-year-select");
const calendarSportSelect = document.querySelector("#calendar-sport-select");
const calendarContainer   = document.querySelector("#calendar-container");
const calendarPopup       = document.querySelector("#calendar-popup");
const calendarPopupDate   = document.querySelector("#calendar-popup-date");
const calendarPopupActs   = document.querySelector("#calendar-popup-activities");
const calendarPopupClose  = document.querySelector("#calendar-popup-close");

const CAL_MONTHS = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"];
const CAL_DAYS   = ["Sun","Mon","Tue","Wed","Thu","Fri","Sat"];
// Activity count → CSS class (0–4 levels, matching app blue palette)
function calLevel(count) {
  if (count === 0) return 0;
  if (count === 1) return 1;
  if (count === 2) return 2;
  if (count === 3) return 3;
  return 4;
}

async function initCalendar() {
  try {
    const [{ years }, { sports }] = await Promise.all([
      fetchJson("/analytics/activity-calendar/years"),
      fetchJson("/analytics/activity-calendar/sports"),
    ]);

    if (!years || years.length === 0) {
      calendarContainer.innerHTML = `<p class="load-prompt">No activity data found. Run <code>python db/build.py</code> first.</p>`;
      return;
    }

    calendarYearSelect.innerHTML = years.map((y) => `<option value="${y}">${y}</option>`).join("");

    if (sports && sports.length > 0) {
      calendarSportSelect.innerHTML =
        `<option value="">All sports</option>` +
        sports.map((s) => `<option value="${s}">${capitalize(s)}</option>`).join("");
    }

    const reloadCalendar = () =>
      loadCalendar(parseInt(calendarYearSelect.value), calendarSportSelect.value || null);

    calendarYearSelect.addEventListener("change", reloadCalendar);
    calendarSportSelect.addEventListener("change", reloadCalendar);

    loadCalendar(years[0], null);
  } catch {
    calendarContainer.innerHTML = `<p class="load-prompt error-text">Could not load calendar. Is the API running?</p>`;
  }
}

async function loadCalendar(year, sport) {
  calendarContainer.innerHTML = `<p class="load-prompt">Loading ${year}…</p>`;
  closeCalendarPopup();
  try {
    const qs = sport ? `?year=${year}&sport=${encodeURIComponent(sport)}` : `?year=${year}`;
    const { days } = await fetchJson(`/analytics/activity-calendar${qs}`);
    renderCalendar(year, days);
  } catch {
    calendarContainer.innerHTML = `<p class="load-prompt error-text">Failed to load calendar data.</p>`;
  }
}

function renderCalendar(year, days) {
  // Build lookup: "YYYY-MM-DD" → {activity_count, sports}
  const dataMap = new Map();
  days.forEach((d) => dataMap.set(String(d.date).slice(0, 10), d));

  // Grid starts on the Sunday on or before Jan 1
  const jan1      = new Date(year, 0, 1);
  const gridStart = new Date(jan1);
  gridStart.setDate(gridStart.getDate() - jan1.getDay()); // back to Sunday

  const dec31   = new Date(year, 11, 31);
  const numWeeks = Math.ceil(((dec31 - gridStart) / 86400000 + 1) / 7);

  // Collect cells and month label positions
  const cells       = [];
  const monthLabels = [];
  let lastMonth     = -1;
  let cur           = new Date(gridStart);

  for (let w = 0; w < numWeeks; w++) {
    for (let d = 0; d < 7; d++) {
      const iso  = cur.toISOString().slice(0, 10);
      const inYear = cur.getFullYear() === year;
      const data   = dataMap.get(iso);
      cells.push({ iso, inYear, count: data ? data.activity_count : 0, sports: data ? data.sports : "" });
      if (inYear && cur.getMonth() !== lastMonth && d === 0) {
        monthLabels.push({ week: w, name: CAL_MONTHS[cur.getMonth()] });
        lastMonth = cur.getMonth();
      }
      cur.setDate(cur.getDate() + 1);
    }
  }

  // ── DOM ──────────────────────────────────────────────────────────────────

  // Month labels row
  const monthRow = document.createElement("div");
  monthRow.className = "cal-month-row";
  monthRow.style.setProperty("--cal-cols", numWeeks);
  const monthFills = Array(numWeeks).fill("");
  monthLabels.forEach(({ week, name }) => { monthFills[week] = name; });
  monthFills.forEach((label) => {
    const el = document.createElement("span");
    el.textContent = label;
    monthRow.appendChild(el);
  });

  // Grid body: day-label column + cell grid
  const body = document.createElement("div");
  body.className = "cal-body";

  // Day labels (show Mon, Wed, Fri only — matches GitHub style)
  const dayLabels = document.createElement("div");
  dayLabels.className = "cal-day-labels";
  CAL_DAYS.forEach((name, i) => {
    const el = document.createElement("span");
    el.textContent = (i === 1 || i === 3 || i === 5) ? name : "";
    dayLabels.appendChild(el);
  });

  // Cell grid
  const grid = document.createElement("div");
  grid.className = "cal-grid";
  grid.style.setProperty("--cal-cols", numWeeks);

  cells.forEach(({ iso, inYear, count, sports }) => {
    const cell = document.createElement("button");
    cell.className = `cal-cell cal-cell--${calLevel(count)}`;
    cell.dataset.date  = iso;
    cell.dataset.count = count;
    cell.setAttribute("aria-label", `${iso}${count > 0 ? `: ${count} ${count === 1 ? "activity" : "activities"}` : ""}`);
    if (!inYear) cell.classList.add("cal-cell--outside");
    if (count > 0) {
      cell.addEventListener("click", (e) => openCalendarPopup(iso, count, sports, e.currentTarget));
    }
    grid.appendChild(cell);
  });

  body.appendChild(dayLabels);
  body.appendChild(grid);

  // Legend
  const legend = document.createElement("div");
  legend.className = "cal-legend";
  legend.innerHTML = `
    <span class="cal-legend-label">Less</span>
    ${[0,1,2,3,4].map((l) => `<span class="cal-cell cal-cell--${l} cal-legend-cell"></span>`).join("")}
    <span class="cal-legend-label">More</span>`;

  calendarContainer.innerHTML = "";
  calendarContainer.appendChild(monthRow);
  calendarContainer.appendChild(body);
  calendarContainer.appendChild(legend);
}

// ── Popup ────────────────────────────────────────────────────────────────────

function closeCalendarPopup() {
  calendarPopup.classList.add("hidden");
}

async function openCalendarPopup(dateStr, count, sports, targetCell) {
  // Position popup near the cell
  const rect    = targetCell.getBoundingClientRect();
  const section = document.querySelector("#calendar");
  const secRect = section.getBoundingClientRect();

  calendarPopup.style.left = `${rect.left - secRect.left}px`;
  // Prefer showing above; fall back to below if near the top
  const above = rect.top - secRect.top - calendarPopup.offsetHeight - 8;
  calendarPopup.style.top  = above > 0
    ? `${above}px`
    : `${rect.bottom - secRect.top + 8}px`;

  calendarPopupDate.textContent = new Date(dateStr + "T12:00:00").toLocaleDateString(undefined, {
    weekday: "long", year: "numeric", month: "long", day: "numeric",
  });
  calendarPopupActs.innerHTML = `<p class="cal-popup-loading">Loading…</p>`;
  calendarPopup.classList.remove("hidden");

  try {
    const { activities } = await fetchJson(`/analytics/activities-for-date?date=${dateStr}`);
    if (!activities || activities.length === 0) {
      calendarPopupActs.innerHTML = `<p class="cal-popup-empty">No activity detail found.</p>`;
      return;
    }
    calendarPopupActs.innerHTML = activities.map((a) => {
      const isRun = (a.sport || "").toLowerCase() === "run";
      const pace  = isRun ? fmtPace(a.avg_speed_ms) : fmtSpeed(a.avg_speed_ms);
      return `
        <div class="cal-popup-activity">
          <div class="cal-popup-activity-header">
            <span class="cal-popup-sport">${capitalize(a.sport || "")}</span>
            <span class="cal-popup-source">${a.data_source}</span>
          </div>
          ${a.name ? `<div class="cal-popup-name">${a.name}</div>` : ""}
          <div class="cal-popup-stats">
            <span>${fmtDistance(a.total_distance_m)}</span>
            <span>${fmtDuration(a.moving_time_s)}</span>
            <span>${pace}</span>
            ${a.avg_heart_rate != null ? `<span>${Math.round(a.avg_heart_rate)} bpm</span>` : ""}
          </div>
        </div>`;
    }).join("");
  } catch {
    calendarPopupActs.innerHTML = `<p class="cal-popup-empty">Failed to load details.</p>`;
  }
}

calendarPopupClose.addEventListener("click", closeCalendarPopup);
document.addEventListener("click", (e) => {
  if (!calendarPopup.classList.contains("hidden") &&
      !calendarPopup.contains(e.target) &&
      !e.target.classList.contains("cal-cell")) {
    closeCalendarPopup();
  }
});

initCalendar();

stravaBackMonths.addEventListener("click", () => showStravaView(stravaViewMonths));
stravaBackActivities.addEventListener("click", () =>
  loadStravaActivitiesForMonth(stravaCurrentYear, stravaCurrentMonth, stravaMonthTitle.textContent)
);

loadStravaMonths();
