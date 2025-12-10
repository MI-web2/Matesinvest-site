// netlify/functions/asx200-latest.js
// Returns latest ASX200 snapshot from Upstash (key: "asx200:latest")

const fetch = (...args) => global.fetch(...args);

exports.handler = async function () {
  const UPSTASH_URL = process.env.UPSTASH_REDIS_REST_URL;
  const UPSTASH_TOKEN = process.env.UPSTASH_REDIS_REST_TOKEN;

  if (!UPSTASH_URL || !UPSTASH_TOKEN) {
    console.error("Upstash not configured");
    return { statusCode: 500, body: "Upstash not configured" };
  }

  const key = "asx200:latest";

  try {
    const res = await fetch(
      `${UPSTASH_URL}/get/${encodeURIComponent(key)}`,
      {
        headers: {
          Authorization: `Bearer ${UPSTASH_TOKEN}`,
        },
      }
    );

    if (!res.ok) {
      const text = await res.text();
      console.error("Upstash GET failed", res.status, text);
      return { statusCode: 500, body: "Failed to fetch asx200:latest" };
    }

    const data = await res.json();
    const raw = data.result ?? data; // Upstash REST returns { result: "json-string" }

    const items = typeof raw === "string" ? JSON.parse(raw) : raw;

    return {
      statusCode: 200,
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ items }),
    };
  } catch (err) {
    console.error("asx200-latest error", err);
    return { statusCode: 500, body: "Internal error" };
  }
};
