// netlify/functions/backfill-email-ids.js
// One-time backfill: assigns sequential IDs MI0000001... to emails in Upstash SET email:subscribers
// Stores mappings:
//   email:id:{email} -> MI0000001
//   id:email:{MI0000001} -> email
// Sets:
//   user:id:counter -> last number assigned
//
// Safe to re-run: will skip emails that already have an ID.

const fetch = (...args) => global.fetch(...args);

const UPSTASH_URL = process.env.UPSTASH_REDIS_REST_URL;
const UPSTASH_TOKEN = process.env.UPSTASH_REDIS_REST_TOKEN;

function fmt(n) {
  return `MI${String(n).padStart(7, "0")}`;
}

async function pipeline(cmds) {
  const res = await fetch(`${UPSTASH_URL}/pipeline`, {
    method: "POST",
    headers: {
      Authorization: `Bearer ${UPSTASH_TOKEN}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify(cmds),
  });

  const text = await res.text();
  let json;
  try {
    json = JSON.parse(text);
  } catch {
    throw new Error(`Upstash pipeline non-JSON response: ${text}`);
  }

  if (!res.ok) {
    throw new Error(`Upstash pipeline failed: ${res.status} ${text}`);
  }

  // Upstash returns an array of { result, error }
  for (const item of json) {
    if (item && item.error) {
      throw new Error(`Upstash command error: ${JSON.stringify(item.error)}`);
    }
  }

  return json;
}

exports.handler = async (event) => {
  if (!UPSTASH_URL || !UPSTASH_TOKEN) {
    return { statusCode: 500, body: "Upstash not configured" };
  }

  // Optional safety gate
  const key = event.queryStringParameters?.key;
  if (process.env.BACKFILL_KEY && key !== process.env.BACKFILL_KEY) {
    return { statusCode: 403, body: "Forbidden" };
  }

  const setKey = event.queryStringParameters?.set || "email:subscribers";

  // 1) Load emails from set
  const smembers = await pipeline([["SMEMBERS", setKey]]);
  const emails = smembers?.[0]?.result || [];
  emails.sort((a, b) => a.localeCompare(b)); // deterministic order

  // 2) Read existing counter
  const counterRes = await pipeline([["GET", "user:id:counter"]]);
  let counter = Number(counterRes?.[0]?.result || 0);

  // 3) Assign IDs (skip existing)
  let assigned = 0;

  // Batch GET existing ids to reduce calls
  // (Do in chunks to avoid huge payloads)
  const chunkSize = 200;

  for (let i = 0; i < emails.length; i += chunkSize) {
    const chunk = emails.slice(i, i + chunkSize);

    // GET existing ids for chunk
    const getCmds = chunk.map((email) => ["GET", `email:id:${email}`]);
    const getRes = await pipeline(getCmds);

    // Build write cmds for missing
    const writeCmds = [];
    for (let j = 0; j < chunk.length; j++) {
      const email = chunk[j];
      const existing = getRes[j]?.result;

      if (existing) continue;

      counter += 1;
      const id = fmt(counter);

      // Use SETNX so reruns are safe (and race-safe)
      writeCmds.push(["SETNX", `email:id:${email}`, id]);
      writeCmds.push(["SET", `id:email:${id}`, email]);

      assigned += 1;
    }

    if (writeCmds.length) {
      await pipeline(writeCmds);
    }
  }

  // 4) Persist counter
  await pipeline([["SET", "user:id:counter", String(counter)]]);

  return {
    statusCode: 200,
    headers: { "Content-Type": "application/json", "Access-Control-Allow-Origin": "*" },
    body: JSON.stringify({
      ok: true,
      setKey,
      total_emails: emails.length,
      newly_assigned: assigned,
      last_id: fmt(counter),
      note: "Assigned in deterministic A→Z email order (no true timestamps available).",
    }),
  };
};
