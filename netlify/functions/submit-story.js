// netlify/functions/submit-story.js
// Accepts a POSTed story, stores it in Upstash as "pending",
// and emails moderators via Resend with approve/reject links.

const fetch = (...args) => global.fetch(...args);

exports.handler = async function (event) {
  const UPSTASH_URL = process.env.UPSTASH_REDIS_REST_URL;
  const UPSTASH_TOKEN = process.env.UPSTASH_REDIS_REST_TOKEN;
  const RESEND_API_KEY = process.env.RESEND_API_KEY;
  const EMAIL_FROM = process.env.EMAIL_FROM || "hello@matesinvest.com";
  const MODERATION_SECRET = process.env.MATES_STORY_MOD_SECRET;
  const MOD_EMAIL_TO = process.env.MATES_STORY_MOD_EMAIL_TO; // "luke@...,dale@..."

  if (event.httpMethod !== "POST") {
    return { statusCode: 405, body: "Method not allowed" };
  }

  if (!UPSTASH_URL || !UPSTASH_TOKEN) {
    console.error("Upstash not configured");
    return { statusCode: 500, body: "Upstash not configured" };
  }
  if (!RESEND_API_KEY) {
    console.error("RESEND_API_KEY missing");
    return { statusCode: 500, body: "Resend not configured" };
  }
  if (!MODERATION_SECRET || !MOD_EMAIL_TO) {
    console.error("Moderation env vars missing");
    return {
      statusCode: 500,
      body: "Moderation not configured",
    };
  }

  try {
    const body = JSON.parse(event.body || "{}");

    const {
      name = "Anon",
      age,
      location,
      title,
      text,
      tag = "User story",
      email,
      consent,
    } = body;

    if (!title || !text || !consent) {
      return {
        statusCode: 400,
        body: JSON.stringify({
          error: "Missing title, text, or consent flag",
        }),
      };
    }

    const now = new Date();
    const id = `story:${now.getTime()}`;

    const story = {
      id,
      tag,
      title,
      text,
      byline: buildByline({ name, age, location }),
      email: email || null,
      consent: !!consent,
      status: "pending",
      createdAt: now.toISOString().slice(0, 10), // YYYY-MM-DD
    };

    const storyJson = JSON.stringify(story);

    // HSET id data <json>
    await upstashPath(
      UPSTASH_URL,
      UPSTASH_TOKEN,
      `/hset/${encodeURIComponent(id)}/${encodeURIComponent(
        "data"
      )}/${encodeURIComponent(storyJson)}`,
      "POST"
    );

    // SADD mates:stories:pending id
    await upstashPath(
      UPSTASH_URL,
      UPSTASH_TOKEN,
      `/sadd/${encodeURIComponent("mates:stories:pending")}/${encodeURIComponent(
        id
      )}`,
      "POST"
    );

    // Send moderation email (Resend)
    await sendModerationEmail({
      RESEND_API_KEY,
      EMAIL_FROM,
      MOD_EMAIL_TO,
      MODERATION_SECRET,
      story,
    });

    return {
      statusCode: 200,
      body: JSON.stringify({ ok: true, id }),
    };
  } catch (err) {
    console.error(
      "submit-story error",
      err && (err.stack || err.message)
    );
    return {
      statusCode: 500,
      body: JSON.stringify({ error: "Internal error" }),
    };
  }
};

// --- Helpers ---

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

function buildByline({ name, age, location }) {
  const bits = [];
  if (name) bits.push(name);
  if (age) bits.push(age);
  if (location) bits.push(location);
  return bits.join(" • ");
}

function escapeHtml(str) {
  return String(str || "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;");
}

async function sendModerationEmail({
  RESEND_API_KEY,
  EMAIL_FROM,
  MOD_EMAIL_TO,
  MODERATION_SECRET,
  story,
}) {
  const baseUrl = process.env.URL || "https://matesinvest.com";

  const approveUrl = `${baseUrl}/.netlify/functions/story-moderate?action=approve&id=${encodeURIComponent(
    story.id
  )}&token=${encodeURIComponent(MODERATION_SECRET)}`;

  const rejectUrl = `${baseUrl}/.netlify/functions/story-moderate?action=reject&id=${encodeURIComponent(
    story.id
  )}&token=${encodeURIComponent(MODERATION_SECRET)}`;

  const subject = `New Mates story: ${story.title}`;
  const html = `
    <h2>New Mates story submitted</h2>
    <p><strong>Title:</strong> ${escapeHtml(story.title)}</p>
    <p><strong>Tag:</strong> ${escapeHtml(story.tag)}</p>
    <p><strong>Byline:</strong> ${escapeHtml(story.byline || "Anon")}</p>
    <p><strong>Text:</strong></p>
    <p>${escapeHtml(story.text).replace(/\n/g, "<br>")}</p>
    <p>
      <a href="${approveUrl}">✅ Approve</a> |
      <a href="${rejectUrl}">❌ Reject</a>
    </p>
  `;

  const toList = MOD_EMAIL_TO.split(",")
    .map((e) => e.trim())
    .filter(Boolean);

  const res = await fetch("https://api.resend.com/emails", {
    method: "POST",
    headers: {
      Authorization: `Bearer ${RESEND_API_KEY}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      from: `MatesInvest <${EMAIL_FROM}>`,
      to: toList,
      subject,
      html,
    }),
  });

  if (!res.ok) {
    const txt = await res.text().catch(() => "");
    console.error("Resend moderation email failed", res.status, txt);
    throw new Error("Failed to send moderation email");
  }
}
