// netlify/functions/test-metals.js
// Minimal debug endpoint: calls Metals-API and exchangerate.host and returns raw responses.
// Purpose: verify METALS_API_KEY is accessible and provider responses reach Netlify.
exports.handler = async function (event) {
  const nowIso = new Date().toISOString();

  async function fetchWithTimeout(url, opts = {}, timeout = 8000) {
    const controller = new AbortController();
    const id = setTimeout(() => controller.abort(), timeout);
    try {
      const res = await fetch(url, { ...opts, signal: controller.signal });
      clearTimeout(id);
      const text = await res.text().catch(() => '');
      let json = null;
      try { json = text ? JSON.parse(text) : null; } catch (e) { json = null; }
      return { ok: res.ok, status: res.status, text: text.slice(0, 2000), json };
    } catch (err) {
      clearTimeout(id);
      return { ok: false, status: null, error: err && err.message ? err.message : String(err) };
    }
  }

  try {
    const METALS_API_KEY = process.env.METALS_API_KEY || null;

    // Metals API call
    let metalsResult = null;
    if (METALS_API_KEY) {
      const url = `https://metals-api.com/api/latest?access_key=${encodeURIComponent(METALS_API_KEY)}&base=USD&symbols=XAU`;
      metalsResult = await fetchWithTimeout(url, {}, 9000);
    } else {
      metalsResult = { ok: false, error: 'METALS_API_KEY not set in env' };
    }

    // FX call
    const fxResult = await fetchWithTimeout('https://api.exchangerate.host/latest?base=USD&symbols=AUD', {}, 8000);

    return {
      statusCode: 200,
      body: JSON.stringify({
        probeAt: nowIso,
        metals: metalsResult,
        fx: fxResult
      }, null, 2)
    };
  } catch (err) {
    return { statusCode: 500, body: JSON.stringify({ error: err && err.message ? err.message : String(err) }) };
  }
};