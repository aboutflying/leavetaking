/**
 * Content script: injects score badges on Amazon search results and product pages.
 */

(function () {
  "use strict";

  const BADGE_CLASS = "pp-score-badge";
  const DETAIL_CLASS = "pp-detail-overlay";

  // Amazon DOM selectors for brand names
  const SELECTORS = {
    // Product page
    productBrand: "#bylineInfo",
    productBrandLink: "#bylineInfo a",
    // Search results
    searchResultBrand: '[data-component-type="s-search-result"] .a-size-base-plus.a-color-base',
    searchResultItem: '[data-component-type="s-search-result"]',
  };

  /**
   * Extract brand name from an Amazon element.
   */
  function extractBrandName(element) {
    let text = element.textContent.trim();
    // Remove common prefixes like "Visit the X Store" or "Brand: X"
    text = text.replace(/^Visit the\s+/i, "").replace(/\s+Store$/i, "");
    text = text.replace(/^Brand:\s*/i, "");
    return text;
  }

  /**
   * Create a score badge element.
   */
  function createBadge(brandName, scoreData) {
    const badge = document.createElement("span");
    badge.className = BADGE_CLASS;

    if (!scoreData.found || !scoreData.issues) {
      badge.classList.add("pp-no-data");
      badge.textContent = "No political data";
      badge.title = `No political spending data found for ${brandName}`;
      return badge;
    }

    const issues = Object.entries(scoreData.issues);
    if (issues.length === 0) {
      badge.classList.add("pp-no-data");
      badge.textContent = "No political data";
      return badge;
    }

    // Show top issue score
    const [topIssue, topData] = issues.sort(
      (a, b) => Math.abs(b[1].score - 50) - Math.abs(a[1].score - 50)
    )[0];

    const score = topData.score;
    badge.classList.add(getScoreClass(score));

    const label = topIssue.replace(/_/g, " ");
    badge.innerHTML = `
      <span class="pp-score-value">${Math.round(score)}</span>
      <span class="pp-score-label">${label}</span>
      <span class="pp-confidence pp-conf-${topData.confidence}">${topData.confidence}</span>
    `;
    badge.title = `${brandName}: ${label} score ${Math.round(score)}/100 (${topData.confidence} confidence). Click for details.`;

    badge.addEventListener("click", (e) => {
      e.preventDefault();
      e.stopPropagation();
      showDetailOverlay(brandName);
    });

    return badge;
  }

  /**
   * Get CSS class based on score value.
   */
  function getScoreClass(score) {
    if (score >= 70) return "pp-score-high";
    if (score >= 40) return "pp-score-mid";
    return "pp-score-low";
  }

  /**
   * Show detail overlay with full money trail.
   */
  function showDetailOverlay(brandName) {
    // Remove existing overlay
    const existing = document.querySelector(`.${DETAIL_CLASS}`);
    if (existing) existing.remove();

    const overlay = document.createElement("div");
    overlay.className = DETAIL_CLASS;
    overlay.innerHTML = `
      <div class="pp-detail-content">
        <div class="pp-detail-header">
          <h2>Political Spending Trail: ${brandName}</h2>
          <button class="pp-close">&times;</button>
        </div>
        <div class="pp-detail-body">
          <p>Loading money trail...</p>
        </div>
      </div>
    `;

    overlay.querySelector(".pp-close").addEventListener("click", () => overlay.remove());
    overlay.addEventListener("click", (e) => {
      if (e.target === overlay) overlay.remove();
    });

    document.body.appendChild(overlay);

    // Fetch trail data
    chrome.runtime.sendMessage(
      { type: "GET_TRAIL", brandName },
      (trail) => {
        const body = overlay.querySelector(".pp-detail-body");
        if (!trail) {
          body.innerHTML = "<p>Could not load trail data. Is the API running?</p>";
          return;
        }
        body.innerHTML = renderTrail(trail);
      }
    );
  }

  /**
   * Render trail data as HTML.
   */
  function renderTrail(trail) {
    let html = "";

    if (trail.pac_trail?.length) {
      html += "<h3>PAC Contributions</h3><table class='pp-trail-table'>";
      html += "<tr><th>Corporation</th><th>Committee</th><th>Amount</th><th>Candidate</th><th>Party</th><th>Issue</th><th>Score</th></tr>";
      for (const row of trail.pac_trail) {
        html += `<tr>
          <td>${row.corporation}</td>
          <td>${row.committee}</td>
          <td>$${Number(row.contribution_amount).toLocaleString()}</td>
          <td>${row.candidate}</td>
          <td>${row.party || "—"}</td>
          <td>${row.issue.replace(/_/g, " ")}</td>
          <td>${Math.round(row.scorecard_score)}</td>
        </tr>`;
      }
      html += "</table>";
    }

    if (trail.executive_trail?.length) {
      html += "<h3>Executive Donations</h3><table class='pp-trail-table'>";
      html += "<tr><th>Corporation</th><th>Executive</th><th>Amount</th><th>Candidate</th><th>Party</th><th>Issue</th><th>Score</th></tr>";
      for (const row of trail.executive_trail) {
        html += `<tr>
          <td>${row.corporation}</td>
          <td>${row.executive} (${row.executive_title})</td>
          <td>$${Number(row.donation_amount).toLocaleString()}</td>
          <td>${row.candidate}</td>
          <td>${row.party || "—"}</td>
          <td>${row.issue.replace(/_/g, " ")}</td>
          <td>${Math.round(row.scorecard_score)}</td>
        </tr>`;
      }
      html += "</table>";
    }

    if (!html) {
      html = "<p>No political spending trail found for this brand.</p>";
    }

    html += `<p class="pp-disclaimer">
      Note: This data reflects disclosed contributions only. 501(c)(4) "dark money"
      donations are not disclosed to the FEC and cannot be tracked.
    </p>`;

    return html;
  }

  /**
   * Process a brand element: look up score and inject badge.
   */
  async function processBrandElement(element) {
    if (element.querySelector(`.${BADGE_CLASS}`)) return; // Already processed

    const brandName = extractBrandName(element);
    if (!brandName || brandName.length < 2) return;

    const scoreData = await new Promise((resolve) => {
      chrome.runtime.sendMessage(
        { type: "GET_BRAND_SCORE", brandName },
        resolve
      );
    });

    const badge = createBadge(brandName, scoreData);
    element.appendChild(badge);
  }

  /**
   * Scan page for brand elements and inject badges.
   */
  function scanPage() {
    // Product page
    const productBrand = document.querySelector(SELECTORS.productBrand);
    if (productBrand) {
      processBrandElement(productBrand);
    }

    // Search results
    const searchBrands = document.querySelectorAll(SELECTORS.searchResultBrand);
    searchBrands.forEach(processBrandElement);
  }

  // Initial scan
  scanPage();

  // Observe DOM changes for dynamic content (infinite scroll, etc.)
  const observer = new MutationObserver((mutations) => {
    for (const mutation of mutations) {
      if (mutation.addedNodes.length) {
        scanPage();
        break;
      }
    }
  });

  observer.observe(document.body, { childList: true, subtree: true });
})();
