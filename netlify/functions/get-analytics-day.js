// netlify/functions/get-analytics-day.js
const fetch = (...args) => global.fetch(...args);

async function redisCmd(cmdArray) {
  const UPSTASH_URL = process.env.UPSTASH_REDIS_REST_URL;
  const UPSTASH_TOKEN = process.env.UPSTASH_REDIS_REST_TOKEN;

  const res = await fetch(`${UPSTASH_URL}/pipeline`, {
    method: "POST",
    headers: {
      Authorization: `Bearer ${UPSTASH_TOKEN}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify([cmdArray]),
  });

  const json = await res.json();
  return json?.[0]?.result;
}

exports.handler = async (event) => {
  const day = (event.queryStringParameters?.day || "").trim();
  if (!day) return { statusCode: 400, body: "Missing ?day=YYYY-MM-DD" };

  const key = `mates:analytics:day:${day}`;
  const data = await redisCmd(["HGETALL", key]);

  // Upstash returns an array alternating [field, value, field, value]
  const out = {};
  if (Array.isArray(data)) {
    for (let i = 0; i < data.length; i += 2) out[data[i]] = Number(data[i + 1] || 0);
  }

  return {
    statusCode: 200,
    headers: { "Content-Type": "application/json", "Cache-Control": "no-store" },
    body: JSON.stringify({ day, ...out }),
  };
};
