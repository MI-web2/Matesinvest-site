// netlify/functions/morning-brief.js
// Hardened morning-brief function: extracts balanced JSON from model output,
// retries once with a stricter prompt if parsing fails, and returns debug output when needed.
//
// Requires env vars: NEWSAPI_KEY and OPENAI_API_KEY
export async function handler(event) {
  try {
    const NEWSAPI_KEY = process.env.NEWSAPI_KEY;
    const OPENAI_API_KEY = process.env.OPENAI_API_KEY;

    if (!NEWSAPI_KEY || !OPENAI_API_KEY) {
      return { statusCode: 500, body: JSON.stringify({ error: 'Missing API keys (NEWSAPI_KEY / OPENAI_API_KEY).' }) };
    }

    const params = event.queryStringParameters || {};
    const region = (params.region || 'au').toLowerCase();

    // Build NewsAPI query
    let q = 'market OR stocks OR ASX OR S&P OR NASDAQ OR futures OR inflation OR rates';
    if (region === 'au') {
      q = 'ASX OR Australia OR ASX200 OR "Australian share" OR market OR stocks';
    } else if (region === 'us') {
      q = 'Wall Street OR S&P OR NASDAQ OR US economy OR stocks OR futures';
    } else {
      q = 'markets OR stocks OR global markets OR futures';
    }

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

    // Build the user prompt
    const prompt = `
You are a market briefing assistant writing "Morning Briefing" for Australian retail investors.
Produce a concise briefing using the input headlines below. Output JSON only with these fields:
- tldr: array of 3 short bullet strings (each 10-18 words max).
- summaryHtml: an HTML fragment (approx 3 short paragraphs, plain text, not lists).
- movers: an array of objects { "name": <string>, "move": <string> } e.g. { "name":"ASX200 Futures","move":"-0.2%" }.
- watchList: array of short strings describing 4 things to watch today (economic prints, earnings, rate decisions).
- headlines: an array of at most 6 objects { "title", "url", "source", "publishedAt" } taken from the input.

Input headlines (JSON array):
${JSON.stringify(articles, null, 2)}

Requirements:
- Keep language plain English, briefly Australian-flavoured (cheeky but professional).
- Keep JSON strictly valid. Use only the keys above. Do not include any commentary, explanation, or extra keys.
- Do NOT include markdown/code fences or any leading/trailing text.
- If you can't find enough "movers", generate sensible high-level market movers (e.g. "Tech earnings", "RBA minutes", "AUD vs USD").
- Limit summaryHtml to about 3 paragraphs and keep each under ~40 words.

Return ONLY the JSON object (no commentary).
`.trim();

    // Helper: call OpenAI chat completion
    async function callOpenAI(messages, temperature = 0.2) {
      const res = await fetch('https://api.openai.com/v1/chat/completions', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Authorization': `Bearer ${OPENAI_API_KEY}`
        },
        body: JSON.stringify({
          model: 'gpt-4o-mini',
          messages,
          max_tokens: 700,
          temperature
        })
      });
      if (!res.ok) {
        const txt = await res.text();
        const err = new Error('OpenAI error: ' + txt);
        err.status = res.status;
        throw err;
      }
      return res.json();
    }

    // Robust JSON extraction: find a balanced JSON object substring.
    // Handles quoted strings and escape sequences.
    function extractBalancedJSON(text) {
      if (!text || typeof text !== 'string') return null;
      // find first '{'
      const first = text.indexOf('{');
      if (first === -1) return null;

      let inString = false;
      let escape = false;
      let depth = 0;
      let start = -1;

      for (let i = first; i < text.length; i++) {
        const ch = text[i];

        if (!inString) {
          if (ch === '{') {
            if (start === -1) start = i;
            depth++;
          } else if (ch === '}') {
            depth--;
            if (depth === 0 && start !== -1) {
              // return substring start..i
              return text.slice(start, i + 1);
            }
          }
          if (ch === '"') {
            inString = true;
            escape = false;
          }
        } else {
          if (escape) {
            escape = false;
            continue;
          }
          if (ch === '\\') {
            escape = true;
            continue;
          }
          if (ch === '"') {
            inString = false;
          }
        }
      }
      return null;
    }

    // Try first request
    const systemMsg = { role: 'system', content: 'You are a JSON generator. Output ONLY valid JSON matching the schema requested. Do not include any explanation, commentary, or code fences.' };
    const userMsg = { role: 'user', content: prompt };

    const firstOpenai = await callOpenAI([systemMsg, userMsg], 0.2);
    const firstContent = firstOpenai.choices?.[0]?.message?.content || '';
    console.log('OpenAI raw output (first):', firstContent);

    // Attempt direct parse
    let parsed = null;
    let modelOutputs = { first: firstContent, retry: null };

    // Trim BOM and whitespace
    const cleanedFirst = (firstContent || '').trim();

    try {
      parsed = JSON.parse(cleanedFirst);
    } catch (e1) {
      // Try to extract balanced JSON substring
      const candidate = extractBalancedJSON(cleanedFirst);
      if (candidate) {
        try {
          parsed = JSON.parse(candidate);
        } catch (e2) {
          parsed = null;
        }
      }
    }

    // If parsed still null, retry once (clarifying prompt, temperature 0.0)
    if (!parsed) {
      console.log('First parse failed, retrying with clarifying prompt...');
      const retryMsg = {
        role: 'user',
        content: `The previous response was not valid JSON. Please return ONLY the JSON object matching the schema (no commentary, no code fences). Here is the same input again:\n\n${prompt}`
      };
      try {
        const retryOpenai = await callOpenAI([systemMsg, retryMsg], 0.0);
        const retryContent = retryOpenai.choices?.[0]?.message?.content || '';
        modelOutputs.retry = retryContent;
        console.log('OpenAI raw output (retry):', retryContent);

        const cleanedRetry = retryContent.trim();
        try {
          parsed = JSON.parse(cleanedRetry);
        } catch (e3) {
          const candidate2 = extractBalancedJSON(cleanedRetry);
          if (candidate2) {
            try {
              parsed = JSON.parse(candidate2);
            } catch (e4) {
              parsed = null;
            }
          }
        }
      } catch (retryErr) {
        console.error('Retry OpenAI call failed', retryErr);
      }
    }

    // If still not parsed, return fallback with raw outputs for debugging
    if (!parsed) {
      const fallback = {
        tldr: articles.slice(0, 3).map(a => a.title || '').filter(Boolean),
        summaryHtml: `<p>Markets - brief unavailable from AI. See top headlines below.</p>`,
        movers: [],
        watchList: [],
        headlines: articles.slice(0, 6),
        // Provide raw model outputs so we can see what the model returned (remove in production)
        modelOutputFirst: modelOutputs.first,
        modelOutputRetry: modelOutputs.retry
      };
      return { statusCode: 200, body: JSON.stringify(fallback) };
    }

    // Ensure headlines include URLs where possible
    if (Array.isArray(parsed.headlines) && parsed.headlines.length) {
      parsed.headlines = parsed.headlines.map(h => {
        if (!h.url) {
          const found = articles.find(a => a.title && h.title && a.title.includes(h.title.slice(0, 20)));
          if (found) h.url = found.url;
        }
        return h;
      });
    } else {
      parsed.headlines = articles.slice(0, 6);
    }

    return {
      statusCode: 200,
      body: JSON.stringify(parsed)
    };

  } catch (err) {
    console.error('morning-brief error', err);
    return { statusCode: 500, body: JSON.stringify({ error: err.message || 'Server error' }) };
  }
}