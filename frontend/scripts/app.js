const projectSnapshot = {
  status: [
    { value: "3", label: "backend datasets stabilized" },
    { value: "2", label: "workspaces now in play" },
    { value: "0", label: "framework dependencies added" },
    { value: "Next", label: "API contract and charts" },
  ],
  datasets: [
    {
      title: "Sleep",
      body: "Nightly records, timestamps, and flattened metrics ready for table and trend views.",
      tags: ["JSON source", "DataFrame output", "Historical archive"],
    },
    {
      title: "Hydration",
      body: "Log entries are structured for daily summaries and future intake visualizations.",
      tags: ["Chronological sort", "Calendar dates", "Export metadata"],
    },
    {
      title: "Activity VO2 Max",
      body: "Training-focused records already fit a comparative analytics panel or athlete profile view.",
      tags: ["Performance metric", "Date parsing", "Future insights"],
    },
  ],
  roadmap: [
    "Add a backend-facing API boundary once the query and response shape is stable.",
    "Replace placeholder cards with charts or tables driven by local sample payloads.",
    "Introduce authenticated upload or data-refresh flows after the data contract is settled.",
  ],
};

const statusGrid = document.querySelector("#status-grid");
const datasetGrid = document.querySelector("#dataset-grid");
const roadmapList = document.querySelector("#roadmap-list");

projectSnapshot.status.forEach((item) => {
  const article = document.createElement("article");
  article.className = "status-card fade-in";
  article.innerHTML = `<strong>${item.value}</strong><p>${item.label}</p>`;
  statusGrid.appendChild(article);
});

projectSnapshot.datasets.forEach((dataset) => {
  const article = document.createElement("article");
  article.className = "dataset-card fade-in";

  const pills = dataset.tags
    .map((tag) => `<span class="pill">${tag}</span>`)
    .join("");

  article.innerHTML = `
    <strong>${dataset.title}</strong>
    <p>${dataset.body}</p>
    <div class="dataset-meta">${pills}</div>
  `;

  datasetGrid.appendChild(article);
});

projectSnapshot.roadmap.forEach((step) => {
  const item = document.createElement("li");
  item.textContent = step;
  roadmapList.appendChild(item);
});
