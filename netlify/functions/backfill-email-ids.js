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

async function redis(cmd) {
  const res = await fetch(`${UPSTASH_URL}/command`, {
    method: "POST",
    headers: {
      Authorization: `Bearer ${UPSTASH_TOKEN}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ command: cmd }),
  });
  const json = await res.json();
  if (json.error) throw new Error(JSON.stringify(json.error));
  return json.result;
}

function fmt(n) {
  return `MI${String(n).padStart(7, "0")}`;
}

exports.handler = async (event) => {
  // Optional: protect this endpoint
  // Call with ?key=YOUR_BACKFILL_KEY and set BACKFILL_KEY in env
  const key = event.queryStringParameters?.key;
  if (process.env.BACKFILL_KEY && key !== process.env.BACKFILL_KEY) {
    return { statusCode: 403, body: "Forbidden" };
  }

  const setKey = event.queryStringParameters?.set || "email:subscribers";

  // 1) Load emails from set
  const emails = (await redis(["SMEMBERS", setKey])) || [];
  emails.sort((a, b) => a.localeCompare(b)); // deterministic order

  // 2) Find the next counter start (if you already ran partially)
  let counter = Number(await redis(["GET", "user:id:counter"])) || 0;

  // 3) Assign IDs to any email missing one
  let assigned = 0;
  for (const email of emails) {
    const existing = await redis(["GET", `email:id:${email}`]);
    if (existing) continue;

    counter += 1;
    const id = fmt(counter);

    // Write both directions
    await redis(["SET", `email:id:${email}`, id]);
    await redis(["SET", `id:email:${id}`, email]);

    assigned += 1;
  }

  // 4) Persist counter
  await redis(["SET", "user:id:counter", String(counter)]);

  return {
    statusCode: 200,
    body: JSON.stringify({
      ok: true,
      setKey,
      total_emails: emails.length,
      newly_assigned: assigned,
      last_id: fmt(counter),
      note:
        "IDs assigned deterministically by sorted email order (NOT true signup time).",
    }),
  };
};
