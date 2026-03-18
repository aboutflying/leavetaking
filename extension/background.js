/**
 * Background service worker for Political Purchaser extension.
 * Handles score cache management and API communication.
 */

const API_BASE = "http://localhost:8000/api/v1";
const CACHE_KEY = "pp_scores";
const PREFS_KEY = "pp_preferences";

// Load scores into local storage on install/update
chrome.runtime.onInstalled.addListener(async () => {
  await refreshScoreCache();
});

// Listen for messages from content script and popup
chrome.runtime.onMessage.addListener((message, sender, sendResponse) => {
  if (message.type === "GET_BRAND_SCORE") {
    getBrandScore(message.brandName).then(sendResponse);
    return true; // async response
  }

  if (message.type === "GET_TRAIL") {
    fetchTrail(message.brandName).then(sendResponse);
    return true;
  }

  if (message.type === "REFRESH_CACHE") {
    refreshScoreCache().then(sendResponse);
    return true;
  }

  if (message.type === "GET_PREFERENCES") {
    getPreferences().then(sendResponse);
    return true;
  }

  if (message.type === "SAVE_PREFERENCES") {
    savePreferences(message.preferences).then(sendResponse);
    return true;
  }
});

async function getBrandScore(brandName) {
  const data = await chrome.storage.local.get(CACHE_KEY);
  const scores = data[CACHE_KEY] || {};
  const brandScores = scores.brands?.[brandName];

  if (brandScores) {
    return { found: true, ...brandScores };
  }

  // Fallback to API if not in cache
  try {
    const resp = await fetch(`${API_BASE}/scores/${encodeURIComponent(brandName)}`);
    if (resp.ok) {
      const result = await resp.json();
      return { found: true, ...result };
    }
  } catch (e) {
    console.warn("API fallback failed for", brandName, e);
  }

  return { found: false };
}

async function fetchTrail(brandName) {
  try {
    const resp = await fetch(`${API_BASE}/trail/${encodeURIComponent(brandName)}`);
    if (resp.ok) {
      return await resp.json();
    }
  } catch (e) {
    console.error("Failed to fetch trail for", brandName, e);
  }
  return null;
}

async function refreshScoreCache() {
  try {
    // Try loading from bundled scores.json first
    const resp = await fetch(chrome.runtime.getURL("scores.json"));
    if (resp.ok) {
      const scores = await resp.json();
      await chrome.storage.local.set({ [CACHE_KEY]: scores });
      return { status: "ok", brandCount: scores.meta?.brand_count || 0 };
    }
  } catch (e) {
    console.warn("Could not load bundled scores, trying API", e);
  }

  // Fallback to API
  try {
    const resp = await fetch(`${API_BASE}/scores?q=`);
    if (resp.ok) {
      const data = await resp.json();
      await chrome.storage.local.set({ [CACHE_KEY]: data });
      return { status: "ok", brandCount: data.count || 0 };
    }
  } catch (e) {
    console.error("Failed to refresh score cache", e);
  }

  return { status: "error" };
}

async function getPreferences() {
  const data = await chrome.storage.local.get(PREFS_KEY);
  return data[PREFS_KEY] || {
    issues: ["environment", "civil_liberties", "labor", "lgbtq_rights", "gun_policy"],
    trusted_scorecards: ["ACLU", "League of Conservation Voters", "AFL-CIO"],
    show_low_confidence: false,
    badge_style: "compact",
  };
}

async function savePreferences(preferences) {
  await chrome.storage.local.set({ [PREFS_KEY]: preferences });
  return { status: "saved" };
}
