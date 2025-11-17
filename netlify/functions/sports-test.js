export async function handler() {
  const API_KEY = process.env.SPORTSDB_API_KEY;
  const today = new Date().toISOString().split("T")[0];

  const url = `https://www.thesportsdb.com/api/v1/json/${API_KEY}/eventsday.php?d=${today}`;
  const response = await fetch(url);
  const data = await response.json();

  const events = data.events || [];

  // Step 2: SIMPLE FILTERING
  const allowedLeagues = [
    "NRL",
    "AFL",
    "NBA",
    "English Premier League",
    "Australian A-League",
    "NBL",
    "Big Bash League"
  ];

  const filtered = events.filter(event =>
    allowedLeagues.includes(event.strLeague)
  );

  // Step 3: MAP to simple MatesFeed card format, include richer fields if available
  const mapped = filtered.map(event => ({
    id: event.idEvent,
    league: event.strLeague,
    sport: event.strSport,
    home: event.strHomeTeam,
    away: event.strAwayTeam,
    homeScore: event.intHomeScore ? Number(event.intHomeScore) : null,
    awayScore: event.intAwayScore ? Number(event.intAwayScore) : null,
    status: event.strStatus || "Scheduled",
    utcTime: event.dateEvent + "T" + (event.strTime || "00:00:00") + "Z",
    localTime: event.strTimestamp || null,

    // extra fields pulled from TheSportsDB event object when present
    venue: event.strVenue || "",
    season: event.strSeason || "",
    description: event.strDescriptionEN || "",
    homeFormation: event.strHomeFormation || "",
    awayFormation: event.strAwayFormation || "",
    homeGoalDetails: event.strHomeGoalDetails || "",
    awayGoalDetails: event.strAwayGoalDetails || "",
    homeLineupGoalkeeper: event.strHomeLineupGoalkeeper || "",
    awayLineupGoalkeeper: event.strAwayLineupGoalkeeper || "",
    homeLineupDefense: event.strHomeLineupDefense || "",
    awayLineupDefense: event.strAwayLineupDefense || "",
    homeLineupMidfield: event.strHomeLineupMidfield || "",
    awayLineupMidfield: event.strAwayLineupMidfield || "",
    homeLineupForward: event.strHomeLineupForward || "",
    awayLineupForward: event.strAwayLineupForward || "",
    homeShots: event.intHomeShots ? Number(event.intHomeShots) : null,
    awayShots: event.intAwayShots ? Number(event.intAwayShots) : null,
    // keep the raw object for future use
    raw: event
  }));

  return {
    statusCode: 200,
    body: JSON.stringify(mapped, null, 2),
  };
}