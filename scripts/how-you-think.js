(() => {
  const QUIZ_ID = "how_you_think_v1";
  const STORAGE_KEY = "mates_quiz_how_you_think_v1";

  const el = (id) => document.getElementById(id);

  const hero = el("hero");
  const quizCard = el("quizCard");
  const resultCard = el("resultCard");

  const startBtn = el("startBtn");
  const shareLinkBtn = el("shareLinkBtn");

  const dotsEl = el("dots");
  const progressText = el("progressText");
  const qTitle = el("qTitle");
  const answersEl = el("answers");
  const restartBtn = el("restartBtn");

  const resultEyebrow = el("resultEyebrow");
  const resultBridge = el("resultBridge");
  const investingBlock = el("investingBlock");
  const seeExamplesBtn = el("seeExamplesBtn");
  const shareBtn = el("shareBtn");
  const copyResultBtn = el("copyResultBtn");

  const toast = el("toast");

  let questions = [];
  let investorTypes = {};
  let presets = {};
  let idx = 0;

  // A/B-safe: store session id for dedupe (no PII)
  const sessionId = getOrMakeSessionId();

  const scores = {
    technical: 0,
    value: 0,
    long_term: 0,
    trader: 0,
    social: 0
  };

  const answers = []; // "A".."E"

  function showToast(msg) {
    toast.textContent = msg || "Copied";
    toast.classList.add("on");
    setTimeout(() => toast.classList.remove("on"), 1400);
  }

  function normalizeUTM(params) {
    const keys = ["utm_source", "utm_medium", "utm_campaign", "utm_content", "utm_term"];
    const out = {};
    keys.forEach(k => {
      const v = params.get(k);
      if (v) out[k.replace("utm_", "")] = v;
    });
    return out;
  }

  function getOrMakeSessionId() {
    const key = "mates_session_id";
    let v = localStorage.getItem(key);
    if (!v) {
      v = (crypto?.randomUUID?.() || ("sid_" + Math.random().toString(16).slice(2) + Date.now()));
      localStorage.setItem(key, v);
    }
    return v;
  }

  function buildDots(total, activeIndex) {
    dotsEl.innerHTML = "";
    for (let i = 0; i < total; i++) {
      const d = document.createElement("div");
      d.className = "dot" + (i <= activeIndex ? " on" : "");
      dotsEl.appendChild(d);
    }
  }

  function resetQuiz() {
    idx = 0;
    answers.length = 0;
    Object.keys(scores).forEach(k => scores[k] = 0);
    restartBtn.style.display = "none";
  }

  function startQuiz() {
    hero.style.display = "none";
    resultCard.classList.remove("on");
    quizCard.classList.add("on");
    resetQuiz();
    renderQuestion();
  }

  function renderQuestion() {
    const q = questions[idx];
    if (!q) return;

    buildDots(questions.length, idx);
    progressText.textContent = `Question ${idx + 1} of ${questions.length}`;
    qTitle.textContent = q.question;

    answersEl.innerHTML = "";
    q.options.forEach((opt, i) => {
      const letter = String.fromCharCode(65 + i); // A-E
      const btn = document.createElement("button");
      btn.className = "answer";
      btn.type = "button";
      btn.innerHTML = `
        <div class="pill">${letter}</div>
        <div>
          <strong>${opt.label}</strong>
          ${opt.hint ? `<div class="hint">${opt.hint}</div>` : ""}
        </div>
      `;
      btn.addEventListener("click", () => chooseAnswer(letter, opt.bucket));
      answersEl.appendChild(btn);
    });
  }

  function chooseAnswer(letter, bucket) {
    answers.push(letter);
    if (scores[bucket] === undefined) scores[bucket] = 0;
    scores[bucket] += 1;

    if (idx < questions.length - 1) {
      idx += 1;
      renderQuestion();
      return;
    }
    finishQuiz();
  }

  function topTwo(scoresObj) {
    const entries = Object.entries(scoresObj).sort((a,b) => b[1] - a[1]);
    const [p, s] = entries;
    if (!p) return { primary: null, secondary: null, tie: false };
    const primary = { key: p[0], score: p[1] };
    const secondary = s ? { key: s[0], score: s[1] } : null;
    const tie = secondary && secondary.score === primary.score;
    return {
      primary: primary.key,
      secondary: (tie ? secondary.key : (secondary?.score > 0 ? secondary.key : null)),
      tie
    };
  }

  function resultText(primaryKey, secondaryKey) {
    const p = investorTypes[primaryKey];
    const s = secondaryKey ? investorTypes[secondaryKey] : null;

    if (!p) return "I did the MatesInvest decision quiz.";
    if (s && secondaryKey !== primaryKey) {
      return `Apparently I’m a mix of “${p.label}” + “${s.label}”. Took 30 seconds.`;
    }
    return `Apparently I’m closest to “${p.label}”. Took 30 seconds.`;
  }

  function finishQuiz() {
    quizCard.classList.remove("on");
    restartBtn.style.display = "inline-flex";

    const params = new URLSearchParams(window.location.search);
    const utm = normalizeUTM(params);

    const { primary, secondary, tie } = topTwo(scores);

    const payload = {
      quiz_id: QUIZ_ID,
      session_id: sessionId,
      completed_at: new Date().toISOString(),
      answers: [...answers],
      scores: { ...scores },
      result: { primary, secondary: (secondary && secondary !== primary ? secondary : null), tie: !!tie },
      utm
    };

    // Store locally (required)
    try { localStorage.setItem(STORAGE_KEY, JSON.stringify(payload)); } catch (_) {}

    // Log backend event (optional / non-blocking)
    try {
      fetch("/.netlify/functions/quizEvent", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload)
      }).catch(() => {});
    } catch (_) {}

    renderResult(payload);
  }

  function renderResult(payload) {
    const primaryKey = payload.result.primary;
    const secondaryKey = payload.result.secondary;

    const p = investorTypes[primaryKey];
    const s = secondaryKey ? investorTypes[secondaryKey] : null;

    resultEyebrow.textContent = "Your result";
    const title = (s && secondaryKey !== primaryKey)
      ? `You’re a mix of: ${p.label} + ${s.label}`
      : `You’re closest to: ${p.label}`;

    el("resultTitle").textContent = title;

    resultBridge.textContent =
      "This isn’t about being “good” or “bad” with money. It’s just how you naturally approach decisions — and why different people invest differently.";

    const lines = [];
    const blocks = [];

    // Primary block
    blocks.push(`<p><strong>${p.label}</strong> — ${p.short}</p>`);
    blocks.push(`<ul class="bullets">${p.investing_points.map(x => `<li>${x}</li>`).join("")}</ul>`);
    blocks.push(`<p class="muted" style="margin:10px 0 0;"><strong>Good to know:</strong> ${p.good_to_know}</p>`);

    // Secondary (if meaningful)
    if (s && secondaryKey !== primaryKey) {
      blocks.push(`<hr style="border:none;border-top:1px solid rgba(255,255,255,0.12);margin:12px 0;">`);
      blocks.push(`<p><strong>${s.label}</strong> — ${s.short}</p>`);
      blocks.push(`<ul class="bullets">${s.investing_points.map(x => `<li>${x}</li>`).join("")}</ul>`);
    }

    investingBlock.innerHTML = blocks.join("");

    // Examples button: choose presets
    const chosenPresets = choosePresetsFor(primaryKey, secondaryKey);
    const presetParam = encodeURIComponent(chosenPresets.join(","));
    seeExamplesBtn.onclick = () => {
      window.location.href = `/equity-screener.html?preset=${presetParam}&src=how-you-think`;
    };

    // Share buttons
    const shareText = resultText(primaryKey, secondaryKey);
    const shareUrl = withResultInUrl(primaryKey, secondaryKey);

    shareBtn.onclick = async () => {
      try {
        if (navigator.share) {
          await navigator.share({
            title: "How you usually make decisions",
            text: shareText,
            url: shareUrl
          });
          return;
        }
      } catch (_) {}
      await copyToClipboard(`${shareText} ${shareUrl}`);
      showToast("Share text copied");
    };

    copyResultBtn.onclick = async () => {
      await copyToClipboard(`${shareText}\n${shareUrl}`);
      showToast("Copied");
    };

    resultCard.classList.add("on");
    hero.style.display = "none";
  }

  function choosePresetsFor(primaryKey, secondaryKey) {
    const type = investorTypes[primaryKey];
    const primaryPresets = (type?.default_presets || []).slice(0, 2);
    if (secondaryKey && secondaryKey !== primaryKey) {
      const type2 = investorTypes[secondaryKey];
      const secondaryPresets = (type2?.default_presets || []).slice(0, 1);
      return [...new Set([...primaryPresets, ...secondaryPresets])].filter(Boolean);
    }
    return primaryPresets.filter(Boolean);
  }

  function withResultInUrl(primaryKey, secondaryKey) {
    const u = new URL(window.location.href);
    u.searchParams.set("r", primaryKey || "");
    if (secondaryKey) u.searchParams.set("r2", secondaryKey);
    return u.toString();
  }

  async function copyToClipboard(text) {
    try {
      await navigator.clipboard.writeText(text);
      return true;
    } catch (_) {
      // fallback
      const ta = document.createElement("textarea");
      ta.value = text;
      document.body.appendChild(ta);
      ta.select();
      document.execCommand("copy");
      document.body.removeChild(ta);
      return true;
    }
  }

  async function loadJSON(path) {
    const res = await fetch(path, { cache: "no-store" });
    if (!res.ok) throw new Error(`Failed to load ${path}`);
    return res.json();
  }

  async function init() {
    // Load config
    const [q, types, p] = await Promise.all([
      loadJSON("/data/quiz_questions_v1.json"),
      loadJSON("/data/investor_types.json"),
      loadJSON("/data/screener_presets.json")
    ]);

    questions = q.questions || [];
    investorTypes = types.types || {};
    presets = p.presets || {};

    // Buttons
    startBtn.addEventListener("click", () => startQuiz());
    shareLinkBtn.addEventListener("click", async () => {
      await copyToClipboard(window.location.href);
      showToast("Link copied");
    });
    restartBtn.addEventListener("click", () => {
      hero.style.display = "block";
      quizCard.classList.remove("on");
      resultCard.classList.remove("on");
    });

    // Deep-link: if user comes with result params, show the hero anyway (share-friendly).
    // Optional: you can auto-start or show a “See my result” later.
  }

  init().catch((e) => {
    console.error(e);
    qTitle.textContent = "Couldn’t load quiz. Please refresh.";
  });

})();
