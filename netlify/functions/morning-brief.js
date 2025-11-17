// netlify/functions/morning-brief.js
// Morning brief with a fast local one-liner fallback so UI always gets something immediately.
// Requires env vars (for optional OpenAI flow): NEWSAPI_KEY and OPENAI_API_KEY
export async function handler(event) {
  try {
    const NEWSAPI_KEY = process.env.NEWSAPI_KEY;
    const OPENAI_API_KEY = process.env.OPENAI_API_KEY || null;

    if (!NEWSAPI_KEY) {
      return { statusCode: 500, body: JSON.stringify({ error: 'Missing NEWSAPI_KEY.' }) };
    }

    const params = event.queryStringParameters || {};
    const region = (params.region || 'au').toLowerCase();

    // Build NewsAPI query (same as before)
    let q = 'market OR stocks OR ASX OR S&P OR NASDAQ OR futures OR inflation OR rates';
    if (region === 'au') q = 'ASX OR Australia OR ASX200 OR "Australian share" OR market OR stocks';
    else if (region === 'us') q = 'Wall Street OR S&P OR NASDAQ OR US economy OR stocks OR futures';

    const newsUrl = `https://newsapi.org/v2/everything?q=${encodeURIComponent(q)}&language=en&pageSize=12&sortBy=publishedAt`;
    const newsRes = await fetch(newsUrl, { headers: { 'X-Api-Key': NEWSAPI_KEY } });

    if (!newsRes.ok) {
      const txt = await newsRes.text();
      return { statusCode: newsRes.status, body: JSON.stringify({ error: 'NewsAPI error', details: txt }) };
    }

    const newsData = await newsRes.json();
    const articles = (newsData.articles || []).map(a => ({
      title: a.title,
      source: a.source && a.source.name,
      url: a.url,
      publishedAt: a.publishedAt,
      description: a.description || ''
    }));

    // Helper: create a short one-liner from top headlines (local, fast)
    function makeOneLiner(list) {
      if (!Array.isArray(list) || list.length === 0) return '';
      // pick up to first 3 distinct short heads
      const tops = list.slice(0, 3).map(x => (x.title || '').replace(/\s+/g, ' ').trim()).filter(Boolean);
      if (!tops.length) return '';
      // join into one readable sentence, keep under ~160 chars
      let s = tops.join(' · ');
      if (s.length > 160) s = s.slice(0, 157) + '...';
      return s;
    }

    const quickOneLiner = makeOneLiner(articles);

    // Prepare a safe headlines-only fallback payload (fast)
    const fallbackPayload = {
      oneLiner: quickOneLiner,
      tldr: articles.slice(0, 3).map(a => a.title || '').filter(Boolean),
      summaryHtml: `<p>Markets - brief unavailable from AI. See top headlines below.</p>`,
      movers: [],
      watchList: [],
      headlines: articles.slice(0, 6)
    };

    // If OpenAI isn't configured, return the quick one-liner immediately (fast path)
    if (!OPENAI_API_KEY) {
      console.log('OPENAI_API_KEY not set — returning quick one-liner fallback.');
      return { statusCode: 200, body: JSON.stringify(fallbackPayload) };
    }

    // If OpenAI configured, attempt a short, single-shot call (short timeout)
    // but always keep the quick one-liner in the fallback so the client can show it immediately.
    async function callOpenAIWithTimeout(messages, timeoutMs = 8000, maxTokens = 350, temperature = 0.0) {
      const controller = new AbortController();
      const id = setTimeout(() => controller.abort(), timeoutMs);
      try {
        const res = await fetch('https://api.openai.com/v1/chat/completions', {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
            'Authorization': `Bearer ${OPENAI_API_KEY}`
          },
          body: JSON.stringify({
            model: 'gpt-4o-mini',
            messages,
            max_tokens: maxTokens,
            temperature
          }),
          signal: controller.signal
        });
        clearTimeout(id);
        if (!res.ok) {
          const txt = await res.text();
          throw new Error('OpenAI error: ' + txt);
        }
        return res.json();
      } catch (err) {
        clearTimeout(id);
        if (err.name === 'AbortError') throw new Error('OpenAI request timeout');
        throw err;
      }
    }

    // Small JSON extractor (best-effort)
    function extractBalancedJSON(text) {
      if (!text || typeof text !== 'string') return null;
      const first = text.indexOf('{');
      if (first === -1) return null;
      let inString = false, escape = false, depth = 0, start = -1;
      for (let i = first; i < text.length; i++) {
        const ch = text[i];
        if (!inString) {
          if (ch === '{') { if (start === -1) start = i; depth++; }
          else if (ch === '}') { depth--; if (depth === 0 && start !== -1) return text.slice(start, i + 1); }
          if (ch === '"') { inString = true; escape = false; }
        } else {
          if (escape) { escape = false; continue; }
          if (ch === '\\') { escape = true; continue; }
          if (ch === '"') { inString = false; }
        }
      }
      return null;
    }

    // Build prompt (kept compact)
    const prompt = `
You are a concise market briefing assistant. Using the input headlines below, return a JSON object with fields:
- tldr: array of 3 one-line bullets
- summaryHtml: short 2-paragraph HTML (plain text)
- movers: array of { "name", "move" }
- watchList: array of short strings (3-5 items)
- headlines: up to 6 objects { "title","url","source","publishedAt" }

Input headlines:
${JSON.stringify(articles, null, 2)}

Return ONLY the JSON object (no commentary or fences).
`.trim();

    const systemMsg = { role: 'system', content: 'You are a JSON generator. Output ONLY valid JSON. No commentary.' };
    const userMsg = { role: 'user', content: prompt };

    // Try one quick OpenAI call; if it completes and returns valid JSON we'll return that payload
    try {
      const openaiRes = await callOpenAIWithTimeout([systemMsg, userMsg], 8000, 300, 0.0);
      const content = openaiRes.choices?.[0]?.message?.content || '';
      console.log('OpenAI raw output (single):', content);
      let parsed = null;
      try {
        parsed = JSON.parse(content.trim());
      } catch (e) {
        const candidate = extractBalancedJSON(content.trim());
        if (candidate) {
          try { parsed = JSON.parse(candidate); } catch (e2) { parsed = null; }
        }
      }

      if (parsed) {
        // Ensure oneLiner is present so client can show a short summary immediately
        parsed.oneLiner = parsed.oneLiner || quickOneLiner || (Array.isArray(parsed.tldr) ? (parsed.tldr[0] || '') : '');
        if (!Array.isArray(parsed.headlines) || !parsed.headlines.length) parsed.headlines = articles.slice(0, 6);
        // Short preview log
        console.log('Returning parsed morning brief (fast).');
        return { statusCode: 200, body: JSON.stringify(parsed) };
      } else {
        console.warn('OpenAI returned but parsing failed — returning fallback with one-liner.');
        return { statusCode: 200, body: JSON.stringify(fallbackPayload) };
      }
    } catch (err) {
      // OpenAI timed out or errored; return the one-liner fallback immediately
      console.warn('OpenAI call failed or timed out:', err.message || err);
      return { statusCode: 200, body: JSON.stringify(fallbackPayload) };
    }

  } catch (err) {
    console.error('morning-brief error', err);
    return { statusCode: 500, body: JSON.stringify({ error: err.message || 'Server error' }) };
  }
}