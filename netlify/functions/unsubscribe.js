// netlify/functions/unsubscribe.js
// Unsubscribe endpoint - removes email from subscriber lists
//
// Usage:
//  GET /.netlify/functions/unsubscribe?email=user@example.com
//  Returns HTML confirmation page

const REDIS_REQUEST_TIMEOUT_MS = 7000;

function escapeHtml(text) {
  const map = {
    '&': '&amp;',
    '<': '&lt;',
    '>': '&gt;',
    '"': '&quot;',
    "'": '&#039;'
  };
  return text.replace(/[&<>"']/g, m => map[m]);
}

exports.handler = async function (event) {
  const UPSTASH_URL = process.env.UPSTASH_REDIS_REST_URL;
  const UPSTASH_TOKEN = process.env.UPSTASH_REDIS_REST_TOKEN;

  if (!UPSTASH_URL || !UPSTASH_TOKEN) {
    return {
      statusCode: 500,
      headers: { "Content-Type": "text/html" },
      body: `
        <!DOCTYPE html>
        <html>
        <head><title>Unsubscribe Error</title></head>
        <body style="font-family: -apple-system, sans-serif; max-width: 600px; margin: 50px auto; padding: 20px;">
          <h1>Configuration Error</h1>
          <p>Unable to process unsubscribe request. Please contact support.</p>
        </body>
        </html>
      `,
    };
  }

  // CORS / preflight
  if (event.httpMethod === "OPTIONS") {
    return {
      statusCode: 204,
      headers: {
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Methods": "GET, OPTIONS",
        "Access-Control-Allow-Headers": "Content-Type",
      },
      body: "",
    };
  }

  let email =
    (event.queryStringParameters && event.queryStringParameters.email) || null;

  if (!email || typeof email !== "string") {
    return {
      statusCode: 400,
      headers: { "Content-Type": "text/html" },
      body: `
        <!DOCTYPE html>
        <html>
        <head>
          <meta charset="utf-8">
          <title>Unsubscribe - MatesInvest</title>
          <style>
            body { 
              font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; 
              max-width: 600px; 
              margin: 50px auto; 
              padding: 20px;
              background-color: #f5f7fb;
            }
            .container {
              background: white;
              padding: 40px;
              border-radius: 12px;
              box-shadow: 0 4px 12px rgba(0,0,0,0.1);
            }
            h1 { color: #002040; margin-top: 0; }
            p { color: #64748b; line-height: 1.6; }
            .error { color: #dc2626; }
          </style>
        </head>
        <body>
          <div class="container">
            <h1>Invalid Request</h1>
            <p class="error">No email address provided. Please use the unsubscribe link from your email.</p>
          </div>
        </body>
        </html>
      `,
    };
  }

  email = email.trim().toLowerCase();

  // Light email validation
  const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
  if (!emailRegex.test(email)) {
    return {
      statusCode: 400,
      headers: { "Content-Type": "text/html" },
      body: `
        <!DOCTYPE html>
        <html>
        <head>
          <meta charset="utf-8">
          <title>Unsubscribe - MatesInvest</title>
          <style>
            body { 
              font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; 
              max-width: 600px; 
              margin: 50px auto; 
              padding: 20px;
              background-color: #f5f7fb;
            }
            .container {
              background: white;
              padding: 40px;
              border-radius: 12px;
              box-shadow: 0 4px 12px rgba(0,0,0,0.1);
            }
            h1 { color: #002040; margin-top: 0; }
            p { color: #64748b; line-height: 1.6; }
            .error { color: #dc2626; }
          </style>
        </head>
        <body>
          <div class="container">
            <h1>Invalid Email</h1>
            <p class="error">The email address format is invalid.</p>
          </div>
        </body>
        </html>
      `,
    };
  }

  async function fetchWithTimeout(url, opts = {}, timeout = REDIS_REQUEST_TIMEOUT_MS) {
    const controller = new AbortController();
    const id = setTimeout(() => controller.abort(), timeout);
    try {
      const res = await fetch(url, { ...opts, signal: controller.signal });
      clearTimeout(id);
      return res;
    } catch (err) {
      clearTimeout(id);
      throw err;
    }
  }

  // Keys for subscriber lists
  const dailyKey = "email:subscribers";
  const appKey = "email:subscribers-App";

  async function srem(keyName) {
    const url =
      `${UPSTASH_URL}/srem/` +
      `${encodeURIComponent(keyName)}/` +
      `${encodeURIComponent(email)}`;

    const res = await fetchWithTimeout(url, {
      method: "POST",
      headers: {
        Authorization: `Bearer ${UPSTASH_TOKEN}`,
      },
    });

    return res;
  }

  try {
    // Remove from both subscriber lists
    const [r1, r2] = await Promise.all([
      srem(dailyKey),
      srem(appKey),
    ]);

    if (!r1.ok || !r2.ok) {
      const txt1 = await r1.text().catch(() => "");
      const txt2 = await r2.text().catch(() => "");
      console.warn("unsubscribe srem failed", r1.status, txt1, r2.status, txt2);
      
      return {
        statusCode: 500,
        headers: { "Content-Type": "text/html" },
        body: `
          <!DOCTYPE html>
          <html>
          <head>
            <meta charset="utf-8">
            <title>Unsubscribe Error - MatesInvest</title>
            <style>
              body { 
                font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; 
                max-width: 600px; 
                margin: 50px auto; 
                padding: 20px;
                background-color: #f5f7fb;
              }
              .container {
                background: white;
                padding: 40px;
                border-radius: 12px;
                box-shadow: 0 4px 12px rgba(0,0,0,0.1);
              }
              h1 { color: #002040; margin-top: 0; }
              p { color: #64748b; line-height: 1.6; }
              .error { color: #dc2626; }
            </style>
          </head>
          <body>
            <div class="container">
              <h1>Error</h1>
              <p class="error">Unable to process unsubscribe request. Please try again later or contact support.</p>
            </div>
          </body>
          </html>
        `,
      };
    }

    // Success
    return {
      statusCode: 200,
      headers: { "Content-Type": "text/html" },
      body: `
        <!DOCTYPE html>
        <html>
        <head>
          <meta charset="utf-8">
          <meta name="viewport" content="width=device-width, initial-scale=1">
          <title>Unsubscribed - MatesInvest</title>
          <style>
            body { 
              font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; 
              max-width: 600px; 
              margin: 50px auto; 
              padding: 20px;
              background-color: #f5f7fb;
            }
            .container {
              background: white;
              padding: 40px;
              border-radius: 12px;
              box-shadow: 0 4px 12px rgba(0,0,0,0.1);
              text-align: center;
            }
            h1 { 
              color: #002040; 
              margin-top: 0;
              font-size: 28px;
            }
            p { 
              color: #64748b; 
              line-height: 1.6;
              font-size: 16px;
            }
            .success { 
              color: #16a34a; 
              font-size: 48px;
              margin-bottom: 20px;
            }
            .email {
              background: #f5f7fb;
              padding: 8px 16px;
              border-radius: 6px;
              font-family: monospace;
              color: #002040;
              display: inline-block;
              margin: 10px 0;
            }
            a {
              color: #3b82f6;
              text-decoration: none;
            }
            a:hover {
              text-decoration: underline;
            }
          </style>
        </head>
        <body>
          <div class="container">
            <div class="success">✓</div>
            <h1>You've been unsubscribed</h1>
            <p>
              <span class="email">${escapeHtml(email)}</span>
            </p>
            <p>You will no longer receive emails from MatesInvest.</p>
            <p style="margin-top: 30px; font-size: 14px;">
              Changed your mind? <a href="https://matesinvest.com/mates-summaries.html#subscribe">Resubscribe here</a>
            </p>
          </div>
        </body>
        </html>
      `,
    };
  } catch (err) {
    console.error("unsubscribe error", err && err.message);
    return {
      statusCode: 500,
      headers: { "Content-Type": "text/html" },
      body: `
        <!DOCTYPE html>
        <html>
        <head>
          <meta charset="utf-8">
          <title>Unsubscribe Error - MatesInvest</title>
          <style>
            body { 
              font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; 
              max-width: 600px; 
              margin: 50px auto; 
              padding: 20px;
              background-color: #f5f7fb;
            }
            .container {
              background: white;
              padding: 40px;
              border-radius: 12px;
              box-shadow: 0 4px 12px rgba(0,0,0,0.1);
            }
            h1 { color: #002040; margin-top: 0; }
            p { color: #64748b; line-height: 1.6; }
            .error { color: #dc2626; }
          </style>
        </head>
        <body>
          <div class="container">
            <h1>Error</h1>
            <p class="error">An unexpected error occurred. Please try again later.</p>
          </div>
        </body>
        </html>
      `,
    };
  }
};
