// Client-side session tracking script for MatesInvest
// Tracks session analytics: session duration, engagement, and sends to track-session function

(function() {
  const KEY_USER = "mates_user_id_v1";
  const KEY_SESSION = "mates_session_id_v1";
  const KEY_SESSION_START = "mates_session_start_v1";
  
  // Engagement criteria
  const ENGAGED_TIME_THRESHOLD = 10; // seconds - minimum time to count as engaged
  const ENGAGED_SCROLL_THRESHOLD = 25; // % - minimum scroll depth
  const ENGAGED_INTERACTION_COUNT = 3; // minimum number of interactions (clicks)
  
  // Session tracking state
  let sessionId = null;
  let sessionStartTime = null;
  let uid = null;
  let interactionCount = 0;
  let maxScrollPercent = 0;
  let isEngaged = false;
  let sessionSent = false;
  
  // Initialize user ID (same as track-visit)
  function initUserId() {
    uid = localStorage.getItem(KEY_USER);
    if (!uid) {
      uid = (crypto?.randomUUID?.() || String(Math.random()).slice(2) + Date.now());
      localStorage.setItem(KEY_USER, uid);
    }
    return uid;
  }
  
  // Initialize or resume session
  function initSession() {
    sessionId = sessionStorage.getItem(KEY_SESSION);
    const storedStartTime = sessionStorage.getItem(KEY_SESSION_START);
    
    if (!sessionId) {
      // New session
      sessionId = (crypto?.randomUUID?.() || String(Math.random()).slice(2) + Date.now());
      sessionStartTime = Date.now();
      sessionStorage.setItem(KEY_SESSION, sessionId);
      sessionStorage.setItem(KEY_SESSION_START, String(sessionStartTime));
    } else {
      // Resume existing session
      sessionStartTime = storedStartTime ? Number(storedStartTime) : Date.now();
    }
    
    return sessionId;
  }
  
  // Calculate session duration in seconds
  function getSessionSeconds() {
    if (!sessionStartTime) return 0;
    return Math.floor((Date.now() - sessionStartTime) / 1000);
  }
  
  // Track scroll depth
  function trackScroll() {
    const scrollTop = window.pageYOffset || document.documentElement.scrollTop;
    const docHeight = Math.max(
      document.body.scrollHeight,
      document.body.offsetHeight,
      document.documentElement.clientHeight,
      document.documentElement.scrollHeight,
      document.documentElement.offsetHeight
    );
    const windowHeight = window.innerHeight;
    const scrollPercent = Math.min(100, (scrollTop / (docHeight - windowHeight)) * 100);
    
    maxScrollPercent = Math.max(maxScrollPercent, scrollPercent);
  }
  
  // Track interactions (clicks)
  function trackInteraction() {
    interactionCount++;
  }
  
  // Determine if session is engaged
  function checkEngagement() {
    const sessionSeconds = getSessionSeconds();
    
    const timeEngaged = sessionSeconds >= ENGAGED_TIME_THRESHOLD;
    const scrollEngaged = maxScrollPercent >= ENGAGED_SCROLL_THRESHOLD;
    const interactionEngaged = interactionCount >= ENGAGED_INTERACTION_COUNT;
    
    // Consider engaged if meeting at least 2 out of 3 criteria
    const criteriaCount = [timeEngaged, scrollEngaged, interactionEngaged].filter(Boolean).length;
    isEngaged = criteriaCount >= 2;
    
    return isEngaged;
  }
  
  // Send session data to backend
  async function sendSessionData() {
    if (sessionSent) return; // Prevent duplicate sends
    if (!uid || !sessionId) return;
    
    const sessionSeconds = getSessionSeconds();
    checkEngagement();
    
    try {
      await fetch("/.netlify/functions/track-session", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          uid,
          sessionId,
          path: window.location.pathname,
          sessionSeconds,
          isEngaged,
          ts: Date.now()
        })
      });
      
      sessionSent = true;
    } catch (err) {
      console.warn("Session tracking failed:", err);
    }
  }
  
  // Send session data before page unload
  function handleBeforeUnload() {
    if (!sessionSent) {
      // Use sendBeacon for reliable delivery during page unload
      const sessionSeconds = getSessionSeconds();
      checkEngagement();
      
      const data = JSON.stringify({
        uid,
        sessionId,
        path: window.location.pathname,
        sessionSeconds,
        isEngaged,
        ts: Date.now()
      });
      
      try {
        navigator.sendBeacon?.("/.netlify/functions/track-session", data);
        sessionSent = true;
      } catch (err) {
        // Fallback to sync fetch if sendBeacon fails
        fetch("/.netlify/functions/track-session", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: data,
          keepalive: true
        }).catch(() => {});
      }
    }
  }
  
  // Send periodic updates for long sessions
  function schedulePeriodicUpdate() {
    setInterval(() => {
      if (!sessionSent && getSessionSeconds() > 0) {
        sendSessionData();
        sessionSent = false; // Allow next update
      }
    }, 30000); // Every 30 seconds
  }
  
  // Initialize tracking
  function init() {
    initUserId();
    initSession();
    
    // Set up event listeners
    window.addEventListener("scroll", trackScroll, { passive: true });
    document.addEventListener("click", trackInteraction, true);
    
    // Send session data on page unload
    window.addEventListener("beforeunload", handleBeforeUnload);
    window.addEventListener("pagehide", handleBeforeUnload);
    
    // Send periodic updates for long sessions
    schedulePeriodicUpdate();
    
    // Track initial scroll position
    trackScroll();
  }
  
  // Start tracking when DOM is ready
  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", init);
  } else {
    init();
  }
})();
