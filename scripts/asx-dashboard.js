/* /scripts/asx-dashboard.js
   ASX Dashboard page logic:
   - Loads Market Pulse summary (cards + movers)
   - Renders Sector performance bar chart with 1D / 5D / 1M toggle
   - Uses Chart.js (loaded via CDN in HTML)
*/

(() => {
  function el(id) {
    return document.getElementById(id);
  }

  function escapeHtml(str) {
    return String(str)
      .replaceAll("&", "&amp;")
      .replaceAll("<", "&lt;")
      .replaceAll(">", "&gt;")
      .replaceAll('"', "&quot;")
      .replaceAll("'", "&#039;");
  }

  function fmtPct(x, dp = 1) {
    if (typeof x !== "number" || !Number.isFinite(x)) return "—";
    const sign = x > 0 ? "+" : "";
    return `${sign}${x.toFixed(dp)}%`;
  }

  function formatAUD(n) {
    if (typeof n !== "number" || !Number.isFinite(n)) return "—";
    const abs = Math.abs(n);
    if (abs >= 1e9) return `$${(n / 1e9).toFixed(2)}B`;
    if (abs >= 1e6) return `$${(n / 1e6).toFixed(2)}M`;
    if (abs >= 1e3) return `$${(n / 1e3).toFixed(0)}k`;
    return `$${Math.round(n)}`;
  }

  function setBar(spanId, pct) {
    const s = el(spanId);
    if (!s) return;
    const clamped =
      typeof pct === "number" && Number.isFinite(pct) ? Math.max(0, Math.min(100, pct)) : 50;
    s.style.width = clamped.toFixed(0) + "%";
  }

  function renderList(listId, rows) {
    const ul = el(listId);
    if (!ul) return;

    ul.innerHTML = "";
    if (!Array.isArray(rows) || rows.length === 0) {
      ul.innerHTML = `<li><span class="code">—</span><span class="pct">—</span></li>`;
      return;
    }

    for (const r of rows) {
      const code = r?.code ? String(r.code) : "—";
      const pct = typeof r?.pct === "number" && Number.isFinite(r.pct) ? r.pct : null;
      const pctText =
        pct == null ? "—" : pct >= 0 ? `+${pct.toFixed(2)}%` : `${pct.toFixed(2)}%`;
      const pctClass = pct == null ? "" : pct >= 0 ? "up" : "down";

      ul.insertAdjacentHTML(
        "beforeend",
        `<li><span class="code">${escapeHtml(code)}</span><span class="pct ${pctClass}">${pctText}</span></li>`
      );
    }
  }

  async function loadMarketPulse() {
    const note = el("mpNote");
    try {
      const res = await fetch("/.netlify/functions/market-pulse-read", { cache: "no-store" });
      const data = await res.json();

      if (!res.ok) throw new Error(data?.error || "Failed to load pulse");

      // Meta
      el("mpAsOf").textContent = data.asOfDate ? `As of ${data.asOfDate}` : "As of —";
      el("mpUniverse").textContent = `Universe: ${data.universeCount ?? "—"}`;

      // Compare note
      if (data.prevDateUsed) {
        note.textContent = `Compare: ${data.prevDateUsed} → ${data.asOfDate || "latest"}`;
      } else {
        note.textContent = data.generatedAt
          ? `Generated: ${String(data.generatedAt).replace("T", " ").slice(0, 19)}`
          : "Loaded from cached snapshot";
      }

      // ASX 200
      if (data.asx200 && typeof data.asx200.pct === "number") {
        el("mpXJO").textContent = fmtPct(data.asx200.pct, 2);
      } else {
        el("mpXJO").textContent = "—";
      }

      // Breadth
      el("mpBreadth").textContent =
        typeof data.breadthPct === "number" ? `${data.breadthPct.toFixed(1)}%` : "—";
      setBar("mpBreadthBar", data.breadthPct);

      // Adv/Dec
      el("mpAD").textContent = `${data.advancers ?? "—"} / ${data.decliners ?? "—"}`;
      el("mpFlat").textContent = `Flat: ${data.flat ?? "—"}`;
      const breadthDen = (data.advancers ?? 0) + (data.decliners ?? 0);
      const adPct = breadthDen > 0 ? ((data.advancers ?? 0) / breadthDen) * 100 : null;
      setBar("mpADBar", adPct);

      // Turnover proxy
      el("mpTurnover").textContent = formatAUD(data.totalTurnoverAud);
      el("mpCoverage").textContent =
        data.turnoverCoverage != null
          ? `Coverage: ${data.turnoverCoverage} stocks (price×volume)`
          : "Coverage: —";

      // Movers
      renderList("mpGainers", data.topGainers);
      renderList("mpLosers", data.topLosers);
    } catch (e) {
      console.warn("Market pulse failed:", e?.message || e);
      if (note) note.textContent = "Pulse unavailable (refresh in a moment)";
    }
  }

function sectorChartModule() {
  const canvas = el("sectorChart");
  const msg = el("sectorChartMessage");
  const meta = el("sectorChartMeta");
  const buttons = document.querySelectorAll(".mi-toggle button");

  if (!canvas) return;

  let chart = null;

  // In-memory cache: period -> { asOf, baseDate, sectors:[{sector,value}] }
  const cache = new Map();
  const inflight = new Map(); // period -> Promise

  function showMessage(text) {
    msg.textContent = text;
    msg.style.display = "block";
    canvas.style.display = "none";
  }
  function hideMessage() {
    msg.style.display = "none";
    canvas.style.display = "block";
  }

  function labelFor(period) {
    if (period === "1d") return "1D";
    if (period === "5d") return "5D";
    if (period === "1m") return "1M";
    return period;
  }

  function applyToChart(json, period) {
    const rows = Array.isArray(json.sectors) ? json.sectors : [];
    if (!rows.length) throw new Error("No sector data");

    const labels = rows.map((r) => r.sector);
    const valuesPct = rows.map((r) => Number(r.value) * 100);

    const bg = valuesPct.map((v) =>
      v >= 0 ? "rgba(34,197,94,0.70)" : "rgba(239,68,68,0.70)"
    );
    const border = valuesPct.map((v) =>
      v >= 0 ? "rgba(34,197,94,1)" : "rgba(239,68,68,1)"
    );

    if (!chart) {
      chart = new Chart(canvas, {
        type: "bar",
        data: {
          labels,
          datasets: [{
            data: valuesPct,
            backgroundColor: bg,
            borderColor: border,
            borderWidth: 1,
            borderRadius: 8,
            barThickness: 14
          }]
        },
        options: {
          responsive: true,
          maintainAspectRatio: false,
          indexAxis: "y",
          animation: false, // 🔥 no anim = faster
          plugins: {
            legend: { display: false },
            tooltip: {
              callbacks: {
                label: (ctx) => {
                  const v = ctx.raw;
                  const sign = v > 0 ? "+" : "";
                  return `${sign}${Number(v).toFixed(2)}%`;
                }
              }
            }
          },
          scales: {
            x: {
              grid: { color: "rgba(15,23,42,0.06)" },
              ticks: { callback: (v) => `${v}%` }
            },
            y: { grid: { display: false } }
          }
        }
      });
    } else {
      // Update in-place (no destroy)
      chart.data.labels = labels;
      chart.data.datasets[0].data = valuesPct;
      chart.data.datasets[0].backgroundColor = bg;
      chart.data.datasets[0].borderColor = border;
      chart.update("none"); // 🔥 no animation
    }

    meta.textContent =
      `${labelFor(period)} · As of ${json.asOf}` +
      (json.baseDate ? ` (vs ${json.baseDate})` : "");
  }

  async function fetchPeriod(period) {
    if (cache.has(period)) return cache.get(period);
    if (inflight.has(period)) return inflight.get(period);

    const p = (async () => {
      const res = await fetch(`/.netlify/functions/get-sector-performance?period=${period}`, {
        cache: "force-cache" // allow browser caching if headers permit
      });
      const json = await res.json().catch(() => ({}));
      if (!res.ok) throw new Error(json?.error || "Not available");
      cache.set(period, json);
      return json;
    })();

    inflight.set(period, p);
    try {
      return await p;
    } finally {
      inflight.delete(period);
    }
  }

  async function load(period) {
    buttons.forEach((b) => b.classList.toggle("active", b.dataset.period === period));
    hideMessage();
    meta.textContent = "Loading…";

    try {
      const json = await fetchPeriod(period);
      applyToChart(json, period);
    } catch (e) {
      showMessage("Not enough history yet for this period.");
      meta.textContent = "—";
    }
  }

  // Prefetch 5d + 1m after first render (makes later clicks instant)
  async function prefetchOthers() {
    await Promise.allSettled([fetchPeriod("5d"), fetchPeriod("1m")]);
  }

  buttons.forEach((btn) => btn.addEventListener("click", () => load(btn.dataset.period)));

  load("1d").then(prefetchOthers);
}

  document.addEventListener("DOMContentLoaded", () => {
    loadMarketPulse();
    sectorChartModule();
  });
})();
