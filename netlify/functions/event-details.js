// netlify/functions/event-details.js
export async function handler(event) {
  try {
    const API_KEY = process.env.SPORTSDB_API_KEY;
    const params = event.queryStringParameters || {};
    const eventId = params.id;
    if (!eventId) {
      return { statusCode: 400, body: JSON.stringify({ error: "Missing id parameter" }) };
    }

    const url = `https://www.thesportsdb.com/api/v1/json/${API_KEY}/lookupevent.php?id=${encodeURIComponent(eventId)}`;
    const res = await fetch(url);
    if (!res.ok) {
      return { statusCode: res.status, body: JSON.stringify({ error: "TheSportsDB returned error" }) };
    }
    const data = await res.json();
    // lookupevent returns { events: [ ... ] } or null
    const ev = (data && data.events && data.events[0]) ? data.events[0] : null;
    if (!ev) {
      return { statusCode: 404, body: JSON.stringify({ error: "Event not found" }) };
    }

    // Return the raw event plus a smaller parsed payload to the client.
    const payload = {
      idEvent: ev.idEvent,
      league: ev.strLeague || "",
      sport: ev.strSport || "",
      home: ev.strHomeTeam || "",
      away: ev.strAwayTeam || "",
      homeScore: ev.intHomeScore !== null ? (ev.intHomeScore !== undefined ? Number(ev.intHomeScore) : null) : null,
      awayScore: ev.intAwayScore !== null ? (ev.intAwayScore !== undefined ? Number(ev.intAwayScore) : null) : null,
      status: ev.strStatus || "Scheduled",
      utcTime: ev.dateEvent ? (ev.dateEvent + "T" + (ev.strTime || "00:00:00") + "Z") : null,
      venue: ev.strVenue || "",
      season: ev.strSeason || "",
      description: ev.strDescriptionEN || "",
      // lineup & goals fields (string blobs in TheSportsDB)
      homeGoalDetails: ev.strHomeGoalDetails || "",
      awayGoalDetails: ev.strAwayGoalDetails || "",
      homeLineupGoalkeeper: ev.strHomeLineupGoalkeeper || "",
      awayLineupGoalkeeper: ev.strAwayLineupGoalkeeper || "",
      homeLineupDefense: ev.strHomeLineupDefense || "",
      awayLineupDefense: ev.strAwayLineupDefense || "",
      homeLineupMidfield: ev.strHomeLineupMidfield || "",
      awayLineupMidfield: ev.strAwayLineupMidfield || "",
      homeLineupForward: ev.strHomeLineupForward || "",
      awayLineupForward: ev.strAwayLineupForward || "",
      homeFormation: ev.strHomeFormation || "",
      awayFormation: ev.strAwayFormation || "",
      homeShots: ev.intHomeShots !== undefined && ev.intHomeShots !== null ? Number(ev.intHomeShots) : null,
      awayShots: ev.intAwayShots !== undefined && ev.intAwayShots !== null ? Number(ev.intAwayShots) : null,
      // include raw object for any additional fields caller may want
      raw: ev
    };

    return {
      statusCode: 200,
      body: JSON.stringify(payload, null, 2),
    };
  } catch (err) {
    console.error("event-details error", err);
    return {
      statusCode: 500,
      body: JSON.stringify({ error: err.message || "Server error" }),
    };
  }
}