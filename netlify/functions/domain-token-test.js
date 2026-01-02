// netlify/functions/domain-token-test.js
const fetch = (...args) => global.fetch(...args);

exports.handler = async () => {
  try {
    const clientId = process.env.DOMAIN_CLIENT_ID;
    const clientSecret = process.env.DOMAIN_CLIENT_SECRET;

    if (!clientId || !clientSecret) {
      return {
        statusCode: 500,
        body: JSON.stringify({ ok: false, error: "Missing DOMAIN_CLIENT_ID or DOMAIN_CLIENT_SECRET" }),
      };
    }

    // Domain OAuth token endpoint (Domain developer portal)
    const tokenUrl = "https://auth.domain.com.au/v1/connect/token";

    const body = new URLSearchParams({
      grant_type: "client_credentials",
      scope: "api_agencies_read api_listings_read", // harmless defaults; Domain may ignore/override
    });

    const res = await fetch(tokenUrl, {
      method: "POST",
      headers: {
        "Content-Type": "application/x-www-form-urlencoded",
        Authorization: "Basic " + Buffer.from(`${clientId}:${clientSecret}`).toString("base64"),
      },
      body: body.toString(),
    });

    const text = await res.text();
    let json;
    try { json = JSON.parse(text); } catch { json = { raw: text }; }

    // Don’t ever return the token itself in logs/UI
    if (!res.ok) {
      return { statusCode: res.status, body: JSON.stringify({ ok: false, error: json }, null, 2) };
    }

    return {
      statusCode: 200,
      body: JSON.stringify({
        ok: true,
        token_type: json.token_type,
        expires_in: json.expires_in,
        scope: json.scope,
        note: "Token minted successfully (token not returned).",
      }),
    };
  } catch (err) {
    return { statusCode: 500, body: JSON.stringify({ ok: false, error: String(err) }) };
  }
};
