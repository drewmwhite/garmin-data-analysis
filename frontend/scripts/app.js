const API_BASE_URL = "http://127.0.0.1:8000/api/v1";
const projectSnapshot = {
  roadmap: [
    "Add filtering and date-range controls on top of the dataset endpoints.",
    "Replace preview tables with charts driven by the same REST payloads.",
    "Introduce endpoint-level caching once the API contract is stable.",
  ],
};

const statusGrid = document.querySelector("#status-grid");
const datasetGrid = document.querySelector("#dataset-grid");
const roadmapList = document.querySelector("#roadmap-list");
const previewGrid = document.querySelector("#preview-grid");
const apiBanner = document.querySelector("#api-banner");

[
  { value: "3", label: "datasets exposed over REST" },
  { value: "1", label: "frontend wired to FastAPI" },
  { value: "GET", label: "endpoints ready for charts" },
  { value: "Live", label: "data previews from backend" },
].forEach((item) => {
  const article = document.createElement("article");
  article.className = "status-card fade-in";
  article.innerHTML = `<strong>${item.value}</strong><p>${item.label}</p>`;
  statusGrid.appendChild(article);
});

projectSnapshot.roadmap.forEach((step) => {
  const item = document.createElement("li");
  item.textContent = step;
  roadmapList.appendChild(item);
});

function renderDatasetCard(dataset) {
  const article = document.createElement("article");
  article.className = "dataset-card fade-in";

  const tags = [
    `${dataset.record_count} records`,
    `${dataset.column_count} columns`,
    ...dataset.sample_columns.slice(0, 2),
  ];

  const pills = tags.map((tag) => `<span class="pill">${tag}</span>`).join("");

  article.innerHTML = `
    <strong>${dataset.title}</strong>
    <p>${dataset.description}</p>
    <div class="dataset-meta">${pills}</div>
  `;

  datasetGrid.appendChild(article);
}

function renderPreviewCard(payload) {
  const article = document.createElement("article");
  article.className = "preview-card fade-in";

  const headers = payload.dataset.sample_columns;
  const rows = payload.records.slice(0, 3);

  const headerMarkup = headers
    .map((header) => `<th scope="col">${header}</th>`)
    .join("");

  const rowMarkup = rows
    .map((row) => {
      const cells = headers
        .map((header) => `<td>${row[header] ?? "—"}</td>`)
        .join("");
      return `<tr>${cells}</tr>`;
    })
    .join("");

  article.innerHTML = `
    <div class="preview-header">
      <div>
        <p class="preview-label">${payload.dataset.slug}</p>
        <h3>${payload.dataset.title}</h3>
      </div>
      <span class="pill">${payload.returned_records} fetched</span>
    </div>
    <div class="preview-table-wrap">
      <table class="preview-table">
        <thead><tr>${headerMarkup}</tr></thead>
        <tbody>${rowMarkup}</tbody>
      </table>
    </div>
  `;

  previewGrid.appendChild(article);
}

function setApiBanner(message, state) {
  apiBanner.textContent = message;
  apiBanner.dataset.state = state;
}

async function fetchJson(path) {
  const response = await fetch(`${API_BASE_URL}${path}`);
  if (!response.ok) {
    throw new Error(`Request failed with status ${response.status}`);
  }
  return response.json();
}

async function loadDatasets() {
  try {
    setApiBanner("Connecting to FastAPI dataset endpoints...", "loading");
    const summaryPayload = await fetchJson("/datasets");

    datasetGrid.innerHTML = "";
    previewGrid.innerHTML = "";

    summaryPayload.datasets.forEach(renderDatasetCard);

    const previewPayloads = await Promise.all(
      summaryPayload.datasets.map((dataset) =>
        fetchJson(`/datasets/${dataset.slug}?limit=3`),
      ),
    );

    previewPayloads.forEach(renderPreviewCard);
    setApiBanner("API connected. Frontend is rendering live backend data.", "success");
  } catch (error) {
    datasetGrid.innerHTML = "";
    previewGrid.innerHTML = "";

    const article = document.createElement("article");
    article.className = "dataset-card";
    article.innerHTML = `
      <strong>API unavailable</strong>
      <p>
        Start the FastAPI server with
        <code>PYTHONPATH=backend/src ./venv/bin/uvicorn api:app --reload</code>
        and refresh this page.
      </p>
    `;
    datasetGrid.appendChild(article);

    setApiBanner(`API connection failed: ${error.message}`, "error");
  }
}

loadDatasets();
