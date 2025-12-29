/* /scripts/asx-dashboard.js
   - Loads Market Pulse summary
   - Renders Sector performance chart
   - Mobile tightening: shorten long sector labels + give chart a little more left space on small screens
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

  function fmtPct(x, dp = 2) {
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

  // Market mood helper (beginner-friendly replacement for "breadth")
  function marketMood(breadthPct) {
    if (typeof breadthPct !== "number" || !Number.isFinite(breadthPct)) {
      return { label: "—", sub: "—", cls: "" };
    }

    // Conservative bands so “Mixed” is common (more intuitive)
    if (breadthPct >= 55) return { label: "Positive", sub: `${breadthPct.toFixed(0)}% of stocks up`, cls: "up" };
    if (breadthPct <= 45) return { label: "Negative", sub: `${breadthPct.toFixed(0)}% of stocks up`, cls: "down" };
    return { label: "Mixed", sub: `${breadthPct.toFixed(0)}% of stocks up`, cls: "neutral" };
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
    try {
      const res = await fetch("/.netlify/functions/market-pulse-read", { cache: "no-store" });
      const data = await res.json();

      if (!res.ok) throw new Error(data?.error || "Failed to load pulse");

      el("mpAsOf").textContent = data.asOfDate ? `As of ${data.asOfDate}` : "As of —";
      el("mpUniverse").textContent = `Universe: ${data.universeCount ?? "—"}`;

      // ASX 200 tile
      if (data.asx200 && typeof data.asx200.pct === "number") {
        el("mpXJO").textContent = fmtPct(data.asx200.pct, 2);
      } else {
        el("mpXJO").textContent = "—";
      }

      // ✅ Market mood tile (replaces breadth)
      const mood = marketMood(data.breadthPct);
      const moodEl = el("mpMood");
      const moodSubEl = el("mpMoodSub");

      if (moodEl) {
        moodEl.textContent = mood.label;

        // Optional: add simple tone classes if you later style them in CSS
        moodEl.classList.remove("up", "down", "neutral");
        if (mood.cls) moodEl.classList.add(mood.cls);
      }
      if (moodSubEl) {
        moodSubEl.textContent = mood.sub;
      }

      // Adv / Dec tile
      el("mpAD").textContent = `${data.advancers ?? "—"} / ${data.decliners ?? "—"}`;

      // Your HTML subtitle is now “Stocks up vs down”
      // Keep a lightweight “Flat: X” line if you want by updating a separate element,
      // but since mpFlat is used for the subtitle now, we won't overwrite it.
      // If you want Flat shown too, add another <div id="mpFlat2"> in HTML.

      const breadthDen = (data.advancers ?? 0) + (data.decliners ?? 0);
      const adPct = breadthDen > 0 ? ((data.advancers ?? 0) / breadthDen) * 100 : null;
      setBar("mpADBar", adPct);

      // Turnover tile
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
    }
  }

  function sectorChartModule() {
    const canvas = el("sectorChart");
    const msg = el("sectorChartMessage");
    const meta = el("sectorChartMeta");
    const buttons = document.querySelectorAll(".mi-toggle button");
    if (!canvas) return;

    let chart = null;
    const cache = new Map();
    const inflight = new Map();

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

    function isMobile() {
      return window.innerWidth <= 640;
    }

    function shortenSectorLabel(label) {
      if (!isMobile() || typeof label !== "string") return label;

      return label
        .replace("Communication Services", "Comm Services")
        .replace("Consumer Defensive", "Cons Defensive")
        .replace("Consumer Cyclical", "Cons Cyclical")
        .replace("Financial Services", "Fin Services")
        .replace("Basic Materials", "Materials");
    }

    function fmtTick(v) {
      const n = Number(v);
      if (!Number.isFinite(n)) return `${v}%`;
      const r = Math.round(n * 10) / 10;
      const clean = Math.abs(r) < 0.05 ? 0 : r;
      return `${clean.toFixed(1)}%`;
    }

    function applyToChart(json, period) {
      const rows = Array.isArray(json.sectors) ? json.sectors : [];
      if (!rows.length) throw new Error("No sector data");

      const labels = rows.map((r) => shortenSectorLabel(r.sector));
      const valuesPct = rows.map((r) => Number(r.value) * 100);

      const bg = valuesPct.map((v) =>
        v >= 0 ? "rgba(34,197,94,0.70)" : "rgba(239,68,68,0.70)"
      );
      const border = valuesPct.map((v) =>
        v >= 0 ? "rgba(34,197,94,1)" : "rgba(239,68,68,1)"
      );

      const baseOptions = {
        responsive: true,
        maintainAspectRatio: false,
        indexAxis: "y",
        animation: false,
        layout: {
          padding: { left: isMobile() ? 10 : 6, right: 8, top: 0, bottom: 0 },
        },
        plugins: {
          legend: { display: false },
          tooltip: {
            callbacks: {
              label: (ctx) => {
                const v = Number(ctx.raw);
                const sign = v > 0 ? "+" : "";
                return `${sign}${v.toFixed(2)}%`;
              },
            },
          },
        },
        scales: {
          x: {
            grid: { color: "rgba(15,23,42,0.06)" },
            ticks: { callback: (v) => fmtTick(v) },
          },
          y: {
            grid: { display: false },
            ticks: { padding: isMobile() ? 6 : 8 },
          },
        },
      };

      if (!chart) {
        chart = new Chart(canvas, {
          type: "bar",
          data: {
            labels,
            datasets: [
              {
                data: valuesPct,
                backgroundColor: bg,
                borderColor: border,
                borderWidth: 1,
                borderRadius: 8,
                barThickness: 14,
              },
            ],
          },
          options: baseOptions,
        });

        window.addEventListener("resize", () => {
          if (!chart) return;
          chart.data.labels = chart.data.labels.map((l) => shortenSectorLabel(l));
          chart.options.layout.padding.left = isMobile() ? 10 : 6;
          chart.options.scales.y.ticks.padding = isMobile() ? 6 : 8;
          chart.update("none");
        });
      } else {
        chart.data.labels = labels;
        chart.data.datasets[0].data = valuesPct;
        chart.data.datasets[0].backgroundColor = bg;
        chart.data.datasets[0].borderColor = border;

        chart.options.layout.padding.left = isMobile() ? 10 : 6;
        chart.options.scales.y.ticks.padding = isMobile() ? 6 : 8;

        chart.update("none");
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
          cache: "force-cache",
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

    buttons.forEach((btn) => btn.addEventListener("click", () => load(btn.dataset.period)));
    load("1d").then(() => Promise.allSettled([fetchPeriod("5d"), fetchPeriod("1m")]));
  }

document.addEventListener("DOMContentLoaded", () => {
  loadMarketPulse();
  sectorChartModule();

  // Collapse "Top movers" on mobile only
  const moversDetails = document.querySelector(".mi-collapse");
  if (moversDetails && window.innerWidth <= 640) {
    moversDetails.removeAttribute("open");
  }
});
})();
