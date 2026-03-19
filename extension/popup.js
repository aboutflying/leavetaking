/**
 * Popup script for Political Purchaser settings.
 */

const ISSUES = [
  { name: "environment", label: "Environment" },
  { name: "civil_liberties", label: "Civil Liberties" },
  { name: "digital_rights", label: "Digital Rights" },
  { name: "labor", label: "Labor" },
  { name: "lgbtq_rights", label: "LGBTQ+ Rights" },
  { name: "immigration", label: "Immigration" },
  { name: "gun_policy", label: "Gun Policy" },
  { name: "healthcare", label: "Healthcare" },
  { name: "education", label: "Education" },
  { name: "fiscal_policy", label: "Fiscal Policy" },
];

const SCORECARDS = [
  { org: "ACLU", issue: "civil_liberties", perspective: "progressive" },
  { org: "League of Conservation Voters", issue: "environment", perspective: "progressive" },
  { org: "Human Rights Campaign", issue: "lgbtq_rights", perspective: "progressive" },
  { org: "AFL-CIO", issue: "labor", perspective: "progressive" },
  { org: "EFF", issue: "digital_rights", perspective: "progressive" },
];

let currentPrefs = null;

document.addEventListener("DOMContentLoaded", async () => {
  currentPrefs = await sendMessage({ type: "GET_PREFERENCES" });
  renderIssues();
  renderScorecards();
  document.getElementById("showLowConfidence").checked =
    currentPrefs.show_low_confidence || false;

  document.getElementById("saveBtn").addEventListener("click", savePreferences);
  document.getElementById("refreshBtn").addEventListener("click", refreshData);
});

function renderIssues() {
  const list = document.getElementById("issueList");
  list.innerHTML = "";
  for (const issue of ISSUES) {
    const li = document.createElement("li");
    const checked = (currentPrefs.issues || []).includes(issue.name);
    li.innerHTML = `
      <input type="checkbox" data-issue="${issue.name}" ${checked ? "checked" : ""}>
      <label>${issue.label}</label>
    `;
    list.appendChild(li);
  }
}

function renderScorecards() {
  const list = document.getElementById("scorecardList");
  list.innerHTML = "";
  for (const sc of SCORECARDS) {
    const li = document.createElement("li");
    const checked = (currentPrefs.trusted_scorecards || []).includes(sc.org);
    li.innerHTML = `
      <div>
        <input type="checkbox" data-scorecard="${sc.org}" ${checked ? "checked" : ""}>
        <label>${sc.org}</label>
      </div>
      <span class="perspective">${sc.perspective}</span>
    `;
    list.appendChild(li);
  }
}

async function savePreferences() {
  const issues = [];
  document.querySelectorAll("[data-issue]").forEach((cb) => {
    if (cb.checked) issues.push(cb.dataset.issue);
  });

  const scorecards = [];
  document.querySelectorAll("[data-scorecard]").forEach((cb) => {
    if (cb.checked) scorecards.push(cb.dataset.scorecard);
  });

  const prefs = {
    issues,
    trusted_scorecards: scorecards,
    show_low_confidence: document.getElementById("showLowConfidence").checked,
    badge_style: "compact",
  };

  await sendMessage({ type: "SAVE_PREFERENCES", preferences: prefs });
  setStatus("Preferences saved!");
}

async function refreshData() {
  setStatus("Refreshing data...");
  const result = await sendMessage({ type: "REFRESH_CACHE" });
  if (result?.status === "ok") {
    setStatus(`Data refreshed! ${result.brandCount} brands loaded.`);
  } else {
    setStatus("Failed to refresh data.");
  }
}

function setStatus(text) {
  const el = document.getElementById("status");
  el.textContent = text;
  setTimeout(() => {
    el.textContent = "";
  }, 3000);
}

function sendMessage(message) {
  return new Promise((resolve) => {
    chrome.runtime.sendMessage(message, resolve);
  });
}
