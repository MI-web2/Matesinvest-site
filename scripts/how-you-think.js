/* /scripts/how-you-think.js
   MatesInvest "How You Think" quiz
   - Non-financey hook
   - 5 questions
   - Scoring into 5 buckets
   - Stores to localStorage
   - Shareable result URL (?r=technical)
   - "See examples" routes to screener preset
*/

(() => {
  const QUIZ_ID = "how_you_think_v1";
  const LS_KEY = `mates_quiz_${QUIZ_ID}`;

  // Update this if your screener page URL is different:
  const SCREENER_URL = "/equity-screener.html";

  const TYPES = {
    technical: {
      label: "The Numbers Person",
      emoji: "🧠",
      oneLiner: "You like logic, patterns, and understanding how things work.",
      investing: [
        "You’ll usually prefer evidence over hype.",
        "Clear data helps you feel confident.",
      ],
      trap: "Overthinking can delay starting.",
      presets: ["technical_growth", "technical_large_cap"],
    },
    value: {
      label: "The Planner",
      emoji: "📋",
      oneLiner: "You like structure, discipline, and getting good value.",
      investing: [
        "You’ll usually care about fundamentals and not overpaying.",
        "You tend to be steady, not flashy.",
      ],
      trap: "Waiting for “perfect” can mean missing “good”.",
      presets: ["value_dividends", "value_quality"],
    },
    long_term: {
      label: "The Long-Game Thinker",
      emoji: "🌱",
      oneLiner: "You’re patient and comfortable letting things play out.",
      investing: [
        "You’ll usually focus on years, not weeks.",
        "Short-term noise won’t bother you much.",
      ],
      trap: "Set-and-forget is great — just don’t switch off completely.",
      presets: ["long_term_quality", "long_term_large_cap"],
    },
    trader: {
      label: "The Fast-Mover",
      emoji: "⚡",
      oneLiner: "You’re decisive and like acting while the moment’s there.",
      investing: [
        "You’ll usually be drawn to momentum and action.",
        "Speed can be a strength if you stay grounded.",
      ],
      trap: "Faster decisions usually come with higher risk — especially early.",
      presets: ["momentum_liquid", "volatility"],
    },
    social: {
      label: "The Talk-it-through Type",
      emoji: "🗣️",
      oneLiner: "You think better out loud and value other perspectives.",
      investing: [
        "You’ll usually learn faster through discussion.",
        "Shared thinking helps you stay calm in noisy moments.",
      ],
      trap: "Consensus helps — clarity matters too.",
      presets: ["popular_large_cap", "most_discussed"],
    },
  };

  // 5 questions, each answer maps to one bucket.
  // Keep copy everyday-life first, money later.
  const QUESTIONS = [
    {
      q: "When something matters to you, what do you rely on most?",
      a: [
        { k: "technical", title: "Logic and numbers", sub: "You like the facts." },
        { k: "value", title: "Whether it feels like good value", sub: "You hate overpaying." },
        { k: "long_term", title: "Time", sub: "You’re happy to let it play out." },
        { k: "trader", title: "Speed", sub: "You act while the moment’s there." },
        { k: "social", title: "People I trust", sub: "You like a second opinion." },
      ],
    },
    {
      q: "If things don’t go to plan, what’s your instinct?",
      a: [
        { k: "technical", title: "Re-check the details", sub: "What changed? What did I miss?" },
        { k: "value", title: "Ask if it was worth it", sub: "Did I pay too much for this?" },
        { k: "long_term", title: "Give it time", sub: "No need to react straight away." },
        { k: "trader", title: "Cut it and move on", sub: "Reset and go again." },
        { k: "social", title: "Get another opinion", sub: "Talk it through first." },
      ],
    },
    {
      q: "Which sentence sounds most like you?",
      a: [
        { k: "technical", title: "I like understanding how things work", sub: "If I get it, I’m calm." },
        { k: "value", title: "I hate overpaying", sub: "Value matters to me." },
        { k: "long_term", title: "I’m patient if it makes sense", sub: "I can wait." },
        { k: "trader", title: "I don’t like waiting around", sub: "I want movement." },
        { k: "social", title: "I think better out loud", sub: "Talking helps me decide." },
      ],
    },
    {
      q: "What makes you most uncomfortable?",
      a: [
        { k: "technical", title: "Not enough info", sub: "I want clarity before I commit." },
        { k: "value", title: "Paying too much", sub: "That feeling annoys me for ages." },
        { k: "long_term", title: "Constantly changing direction", sub: "I prefer a steady plan." },
        { k: "trader", title: "Sitting still too long", sub: "I want something happening." },
        { k: "social", title: "Doing it alone", sub: "I like shared thinking." },
      ],
    },
    {
      q: "When you make a good decision, it’s usually because you:",
      a: [
        { k: "technical", title: "Thought it through logically", sub: "You trust reasoning." },
        { k: "value", title: "Stayed disciplined", sub: "You kept it sensible." },
        { k: "long_term", title: "Played the long game", sub: "You didn’t rush it." },
        { k: "trader", title: "Moved at the right time", sub: "Timing matters to you." },
        { k: "social", title: "Listened to others", sub: "You value perspective." },
      ],
    },
  ];

  // DOM
  const hero = document.getElementById("hero");
  const startBtn = document.getElementById("startBtn");
  const shareLinkBtn = document.getElementById("shareLinkBtn");

  const quizCard = document.getElementById("quizCard");
  const dotsEl = document.getElementById("dots");
  const progressText = document.getElementById("progressText");
  const qTitle = document.getElementById("qTitle");
  const answersEl = document.getElementById("answers");
  const restartBtn = document.getElementById("restartBtn");

  const resultCard = document.getElementById("resultCard");
  const resultBridge = document.getElementById("resultBridge");
  const resultGrid = document.getElementById("resultGrid");
  const seeExamplesBtn = document.getElementById("seeExamplesBtn");
  const shareBtn = document.getElementById("shareBtn");
  const copyResultBtn = document.getElementById("copyResultBtn");

  // State
  let idx = 0;
  let scores = resetScores();
  let answers = [];

  function resetScores() {
    return { technical: 0, value: 0, long_term: 0, trader: 0, social: 0 };
  }

  function buildDots() {
    dotsEl.innerHTML = "";
    for (let i = 0; i < QUESTIONS.length; i++) {
      const d = document.createElement("div");
      d.className = "dot" + (i === idx ? " on" : "");
      dotsEl.appendChild(d);
    }
  }

  function setDotsActive() {
    [...dotsEl.children].forEach((d, i) => {
      d.classList.toggle("on", i === idx);
    });
  }

  function renderQuestion() {
    const q = QUESTIONS[idx];
    progressText.textContent = `Question ${idx + 1} of ${QUESTIONS.length}`;
    setDotsActive();

    qTitle.textContent = q.q;
    answersEl.innerHTML = "";

    q.a.forEach((opt, i) => {
      const btn = document.createElement("div");
      btn.className = "answer";
      btn.setAttribute("role", "button");
      btn.setAttribute("tabindex", "0");

      btn.innerHTML = `
        <div class="pill">${String.fromCharCode(65 + i)}</div>
        <div>
          <strong>${opt.title}</strong>
          <span>${opt.sub}</span>
        </div>
      `;

      const pick = () => handleAnswer(opt.k, String.fromCharCode(65 + i));

      btn.addEventListener("click", pick);
      btn.addEventListener("keydown", (e) => {
        if (e.key === "Enter" || e.key === " ") pick();
      });

      answersEl.appendChild(btn);
    });
  }

  function handleAnswer(bucket, letter) {
    scores[bucket] += 1;
    answers.push(letter);

    if (idx < QUESTIONS.length - 1) {
      idx += 1;
      renderQuestion();
      return;
    }

    // Finish
    const result = calculateResult(scores);
    persistResult(result);
    showResult(result);
  }

  function calculateResult(s) {
    const entries = Object.entries(s).sort((a, b) => b[1] - a[1]);
    const primary = entries[0][0];
    const primaryScore = entries[0][1];

    // Secondary only if tied or close (makes it feel human)
    const second = entries[1][0];
    const secondScore = entries[1][1];

    const secondary = (secondScore === primaryScore) ? second : null;

    return {
      quiz_id: QUIZ_ID,
      completed_at: new Date().toISOString(),
      answers,
      scores: s,
      result: { primary, secondary },
    };
  }

  function persistResult(payload) {
    try {
      localStorage.setItem(LS_KEY, JSON.stringify(payload));
    } catch (_) {}

    // Build shareable URL param
    const u = new URL(window.location.href);
    u.searchParams.set("r", payload.result.primary);
    window.history.replaceState({}, "", u.toString());
  }

  function showQuiz() {
    hero.style.display = "none";
    quizCard.classList.add("on");
    resultCard.classList.remove("on");
    restartBtn.style.display = "inline-flex";
    idx = 0;
    scores = resetScores();
    answers = [];
    buildDots();
    renderQuestion();
  }

  function showResult(payload) {
    quizCard.classList.remove("on");
    resultCard.classList.add("on");

    const primary = payload.result.primary;
    const secondary = payload.result.secondary;

    const p = TYPES[primary];
    const s = secondary ? TYPES[secondary] : null;

    // Bridge copy: friendly + Aussie tone, not finance-forward
    resultBridge.textContent = secondary
      ? `You’re a mix of ${p.label} and ${s.label}. That combo is more common than you think.`
      : `You’re closest to ${p.label}. No good or bad — just how you tend to think.`;

    // Build result blocks
    resultGrid.innerHTML = "";
    [p, s].filter(Boolean).forEach((t) => {
      const el = document.createElement("div");
      el.className = "result-chip";
      el.innerHTML = `
        <div class="result-emoji">${t.emoji}</div>
        <div>
          <strong>${t.label}</strong>
          <p>${t.oneLiner}</p>
          <p style="margin-top:6px;"><span style="color:var(--muted)">Money & investing:</span> ${t.investing.join(" ")}</p>
          <p style="margin-top:6px;"><span style="color:var(--muted)">Watch for:</span> ${t.trap}</p>
        </div>
      `;
      resultGrid.appendChild(el);
    });

    // Wire examples button
    seeExamplesBtn.onclick = () => {
      const presetList = secondary
        ? [...new Set([...p.presets, ...s.presets])]
        : p.presets;

      const url = new URL(window.location.origin + SCREENER_URL);
      url.searchParams.set("preset", presetList.join(","));
      window.location.href = url.toString();
    };

    // Share actions
    const shareUrl = new URL(window.location.href);
    shareUrl.searchParams.set("r", primary);

    const shareText = buildShareText(primary, secondary);
    shareBtn.onclick = async () => shareSmart(shareText, shareUrl.toString());
    copyResultBtn.onclick = async () => copyToClipboard(shareText + " " + shareUrl.toString());
  }

  function buildShareText(primary, secondary) {
    const p = TYPES[primary];
    const s = secondary ? TYPES[secondary] : null;

    const openers = [
      "This took 30 seconds and was weirdly accurate.",
      "Did this for a laugh — it actually nailed me.",
      "Okay… this describes my brain a bit too well 😂",
      "Quick one: how do you make decisions?",
    ];

    const closer = secondary
      ? `I got: ${p.label} + ${s.label}.`
      : `I got: ${p.label}.`;

    return `${openers[Math.floor(Math.random() * openers.length)]} ${closer}`;
  }

  async function shareSmart(text, url) {
    try {
      if (navigator.share) {
        await navigator.share({
          title: "How do you usually make decisions?",
          text,
          url,
        });
        return;
      }
    } catch (_) {
      // fall back to copy
    }
    await copyToClipboard(text + " " + url);
  }

  async function copyToClipboard(str) {
    try {
      await navigator.clipboard.writeText(str);
      toast("Copied ✔");
    } catch (_) {
      // Fallback
      const ta = document.createElement("textarea");
      ta.value = str;
      document.body.appendChild(ta);
      ta.select();
      document.execCommand("copy");
      document.body.removeChild(ta);
      toast("Copied ✔");
    }
  }

  function toast(msg) {
    // tiny minimal toast, no CSS dependency
    const t = document.createElement("div");
    t.textContent = msg;
    t.style.position = "fixed";
    t.style.left = "50%";
    t.style.bottom = "18px";
    t.style.transform = "translateX(-50%)";
    t.style.padding = "10px 12px";
    t.style.borderRadius = "12px";
    t.style.background = "rgba(15,23,42,0.92)";
    t.style.color = "#fff";
    t.style.fontSize = "13px";
    t.style.zIndex = "9999";
    t.style.boxShadow = "0 12px 30px rgba(0,0,0,0.25)";
    document.body.appendChild(t);
    setTimeout(() => {
      t.style.opacity = "0";
      t.style.transition = "opacity 180ms ease";
    }, 900);
    setTimeout(() => t.remove(), 1200);
  }

  function getParam(name) {
    const u = new URL(window.location.href);
    return u.searchParams.get(name);
  }

  function tryShowSharedResult() {
    const r = getParam("r");
    if (!r || !TYPES[r]) return false;

    // If someone opens shared link, show result immediately (viral loop)
    const payload = {
      quiz_id: QUIZ_ID,
      completed_at: new Date().toISOString(),
      answers: [],
      scores: {},
      result: { primary: r, secondary: null },
    };
    hero.style.display = "none";
    quizCard.classList.remove("on");
    resultCard.classList.add("on");
    showResult(payload);
    return true;
  }

  function init() {
    // Copy link button (always copies the clean URL without random params except r if present)
    shareLinkBtn.addEventListener("click", async () => {
      const u = new URL(window.location.href);
      // keep r if present, remove anything else later if you add it
      await copyToClipboard(u.toString());
    });

    startBtn.addEventListener("click", showQuiz);
    restartBtn.addEventListener("click", () => {
      // remove r param when restarting
      const u = new URL(window.location.href);
      u.searchParams.delete("r");
      window.history.replaceState({}, "", u.toString());
      hero.style.display = "";
      quizCard.classList.remove("on");
      resultCard.classList.remove("on");
      restartBtn.style.display = "none";
      idx = 0;
      scores = resetScores();
      answers = [];
    });

    buildDots();

    // If someone came from a share link, show the result directly
    if (tryShowSharedResult()) return;

    // Otherwise, if they’ve done it before, you could optionally show “Continue” / “See your result”
    // Keeping it simple for v1: do nothing.
  }

  init();
})();
