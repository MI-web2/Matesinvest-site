// netlify/functions/story-moderate.js
const fetch = (...args) => global.fetch(...args);

exports.handler = async function (event) {
  const UPSTASH_URL = process.env.UPSTASH_REDIS_REST_URL;
  const UPSTASH_TOKEN = process.env.UPSTASH_REDIS_REST_TOKEN;
  const MODERATION_SECRET = process.env.MATES_STORY_MOD_SECRET;

  if (!UPSTASH_URL || !UPSTASH_TOKEN || !MODERATION_SECRET) {
    return htmlResponse(
      500,
      "<h2>Config error</h2><p>Upstash or secret not set.</p>"
    );
  }

  const { id, action, token } = event.queryStringParameters || {};

  if (!id || !action || !token) {
    return htmlResponse(
      400,
      "<h2>Missing fields</h2><p>id, action, and token are required.</p>"
    );
  }

  if (token !== MODERATION_SECRET) {
    return htmlResponse(
      403,
      "<h2>Not authorised</h2><p>Invalid moderation token.</p>"
    );
  }

  if (!["approve", "reject"].includes(action)) {
    return htmlResponse(
      400,
      "<h2>Bad action</h2><p>Action must be approve or reject.</p>"
    );
  }

  try {
    // HGET id data
    const getRes = await upstashPath(
      UPSTASH_URL,
      UPSTASH_TOKEN,
      `/hget/${encodeURIComponent(id)}/${encodeURIComponent("data")}`,
      "GET"
    );
    const raw = getRes && getRes.result;
    if (!raw) {
      return htmlResponse(
        404,
        "<h2>Not found</h2><p>Story not found.</p>"
      );
    }

    const story = JSON.parse(raw);
    story.status = action === "approve" ? "approved" : "rejected";

    // HSET id data <json>
    await upstashPath(
      UPSTASH_URL,
      UPSTASH_TOKEN,
      `/hset/${encodeURIComponent(id)}/${encodeURIComponent(
        "data"
      )}/${encodeURIComponent(JSON.stringify(story))}`,
      "POST"
    );

    // SREM pending
    await upstashPath(
      UPSTASH_URL,
      UPSTASH_TOKEN,
      `/srem/${encodeURIComponent("mates:stories:pending")}/${encodeURIComponent(
        id
      )}`,
      "POST"
    );

    if (action === "approve") {
      await upstashPath(
        UPSTASH_URL,
        UPSTASH_TOKEN,
        `/sadd/${encodeURIComponent(
          "mates:stories:approved"
        )}/${encodeURIComponent(id)}`,
        "POST"
      );
    } else {
      await upstashPath(
        UPSTASH_URL,
        UPSTASH_TOKEN,
        `/sadd/${encodeURIComponent(
          "mates:stories:rejected"
        )}/${encodeURIComponent(id)}`,
        "POST"
      );
    }

    const nice = action === "approve" ? "approved ✅" : "rejected 👌";
    return htmlResponse(
      200,
      `<h2>Story ${nice}</h2><p>You can close this tab now.</p>`
    );
  } catch (err) {
    console.error("story-moderate error", err && (err.stack || err.message));
    return htmlResponse(
      500,
      "<h2>Server error</h2><p>Check Netlify logs.</p>"
    );
  }
};

// helpers
async function upstashPath(baseUrl, token, path, method = "GET") {
  const url = `${baseUrl}${path}`;
  const res = await fetch(url, {
    method,
    headers: {
      Authorization: `Bearer ${token}`,
    },
  });

  if (!res.ok) {
    const txt = await res.text().catch(() => "");
    console.error("Upstash path failed", method, path, res.status, txt);
    throw new Error("Upstash command failed");
  }
  return res.json().catch(() => null);
}

function htmlResponse(statusCode, innerHtml) {
  return {
    statusCode,
    headers: { "Content-Type": "text/html; charset=utf-8" },
    body: `<!doctype html><html><body style="font-family:system-ui;padding:20px">${innerHtml}</body></html>`,
  };
}
