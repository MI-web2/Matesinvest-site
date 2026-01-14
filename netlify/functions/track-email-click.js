// netlify/functions/track-email-click.js
// Tracks clicks from email links for the referral program.
// Filters out known email scanners for accurate metrics.
// Records: userId, email type, link/path, timestamp to Upstash.

const fetch = (...args) => global.fetch(...args);

const UPSTASH_URL = process.env.UPSTASH_REDIS_REST_URL;
const UPSTASH_TOKEN = process.env.UPSTASH_REDIS_REST_TOKEN;

// Known email scanner User-Agents to filter out
const EMAIL_SCANNERS = [
  'GoogleImageProxy',
  'Gmail Image Proxy',
  'Apple Mail Link Preview',
  'Outlook-iOS-Android',
  'Microsoft Office Existence Discovery',
  'SafariWebView',
  'Mail.RuSputnik',
  'Yahoo! Slurp',
  'SkypeUriPreview',
  'Slack-ImgProxy',
  'LinkedInBot',
  'facebookexternalhit',
  'WhatsApp',
  'TelegramBot',
  'ia_archiver',
];

function getAESTDateString(ts = Date.now()) {
  // AEST = UTC+10 (no DST handling).
  const d = new Date(ts + 10 * 60 * 60 * 1000);
  return d.toISOString().slice(0, 10); // YYYY-MM-DD
}

function isEmailScanner(userAgent) {
  if (!userAgent) return false;
  const ua = userAgent.toLowerCase();
  return EMAIL_SCANNERS.some(scanner => ua.includes(scanner.toLowerCase()));
}

async function redisPipeline(commands) {
  if (!UPSTASH_URL || !UPSTASH_TOKEN) {
    throw new Error("Upstash not configured");
  }

  const res = await fetch(`${UPSTASH_URL}/pipeline`, {
    method: "POST",
    headers: {
      Authorization: `Bearer ${UPSTASH_TOKEN}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify(commands),
  });

  if (!res.ok) {
    const text = await res.text();
    throw new Error(`Upstash error ${res.status}: ${text}`);
  }

  return res.json();
}

async function redisGet(key) {
  if (!UPSTASH_URL || !UPSTASH_TOKEN) {
    throw new Error("Upstash not configured");
  }

  const url = `${UPSTASH_URL}/get/${encodeURIComponent(key)}`;
  const res = await fetch(url, {
    headers: { Authorization: `Bearer ${UPSTASH_TOKEN}` },
  });

  if (!res.ok) return null;
  const j = await res.json();
  return j?.result || null;
}

exports.handler = async function (event) {
  // CORS preflight
  if (event.httpMethod === "OPTIONS") {
    return {
      statusCode: 200,
      headers: {
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Headers": "Content-Type",
        "Access-Control-Allow-Methods": "GET, OPTIONS",
      },
      body: "",
    };
  }

  if (!UPSTASH_URL || !UPSTASH_TOKEN) {
    console.error("Upstash not configured");
    return {
      statusCode: 500,
      body: "Configuration error",
    };
  }

  // Get parameters from query string
  const params = event.queryStringParameters || {};
  const userId = (params.uid || "").trim();
  const emailType = (params.type || "unknown").trim(); // daily-brief, daily-quiz, week-ahead, weekly-brief
  const targetUrl = (params.url || "https://matesinvest.com").trim();

  // Check User-Agent to filter email scanners
  const userAgent = event.headers["user-agent"] || event.headers["User-Agent"] || "";
  
  if (isEmailScanner(userAgent)) {
    console.log("Email scanner detected, not recording click:", userAgent);
    // Still redirect but don't record
    return {
      statusCode: 302,
      headers: {
        Location: targetUrl,
      },
      body: "",
    };
  }

  // Validate userId
  if (!userId || userId.length < 8 || !userId.startsWith("MI")) {
    console.warn("Invalid or missing userId:", userId);
    // Still redirect but don't record
    return {
      statusCode: 302,
      headers: {
        Location: targetUrl,
      },
      body: "",
    };
  }

  try {
    const ts = Date.now();
    const day = getAESTDateString(ts);
    
    // Verify userId exists (get email from id)
    const email = await redisGet(`id:email:${userId}`);
    
    if (!email) {
      console.warn("UserId not found in database:", userId);
      // Still redirect but don't record
      return {
        statusCode: 302,
        headers: {
          Location: targetUrl,
        },
        body: "",
      };
    }

    // Parse the target URL to get the path
    let path = "/";
    try {
      const urlObj = new URL(targetUrl);
      path = urlObj.pathname || "/";
    } catch (e) {
      // Invalid URL, use full string as path
      path = targetUrl;
    }

    // Store click data in Redis
    const dayKey = `email:clicks:day:${day}`;
    const userKey = `email:clicks:user:${userId}`;
    const emailTypeKey = `email:clicks:type:${emailType}:${day}`;
    const pathKey = `email:clicks:path:${day}`;

    const commands = [
      // Overall daily click count
      ["HINCRBY", dayKey, "total_clicks", 1],
      
      // Clicks by email type for this day
      ["HINCRBY", dayKey, `${emailType}_clicks`, 1],
      
      // User-specific click count
      ["HINCRBY", userKey, "total_clicks", 1],
      ["HSET", userKey, "last_click", ts],
      ["HSET", userKey, "last_email_type", emailType],
      
      // Email type specific tracking
      ["HINCRBY", emailTypeKey, "clicks", 1],
      
      // Path/link tracking
      ["HINCRBY", pathKey, path, 1],
      
      // Store detailed click record in a list (limited to last 1000 per user)
      ["LPUSH", `${userKey}:history`, JSON.stringify({
        ts,
        type: emailType,
        path,
        day,
      })],
      ["LTRIM", `${userKey}:history`, 0, 999],
    ];

    await redisPipeline(commands);

    console.log(`Recorded click: userId=${userId}, type=${emailType}, path=${path}`);

    // Redirect to target URL
    return {
      statusCode: 302,
      headers: {
        Location: targetUrl,
      },
      body: "",
    };
  } catch (err) {
    console.error("track-email-click error:", err);
    // On error, still redirect to target
    return {
      statusCode: 302,
      headers: {
        Location: targetUrl,
      },
      body: "",
    };
  }
};
