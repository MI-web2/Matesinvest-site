# Session Tracking Analytics Implementation

This document describes the session tracking analytics implementation for MatesInvest.

## Overview

The session tracking system captures three key metrics across all pages:
1. **session_count** - Total number of unique sessions per day
2. **session_seconds_total** - Total time spent across all sessions (in seconds)
3. **engaged_sessions** - Number of sessions that meet engagement criteria

## Architecture

### Components

1. **Netlify Function**: `/netlify/functions/track-session.js`
   - Receives session data from client
   - Writes metrics to Upstash Redis
   - Tracks both global and per-page metrics

2. **Client Script**: `/scripts/track-session.js`
   - Tracks session start/end times
   - Monitors user engagement (scroll depth, clicks, time on page)
   - Sends data to Netlify function on page unload or periodically

3. **Visit Tracking**: Existing `/netlify/functions/track-visit.js`
   - Already integrated across all pages
   - Tracks page visits and unique users

## Engagement Criteria

A session is considered "engaged" if it meets **at least 2 of the following 3 criteria**:

1. **Time threshold**: ≥ 10 seconds on page
2. **Scroll threshold**: ≥ 25% scroll depth
3. **Interaction threshold**: ≥ 3 clicks/interactions

## Data Storage (Upstash Redis)

### Keys Structure

For a given day (e.g., `2026-01-02`):

#### Global Metrics
- **Key**: `mates:analytics:day:2026-01-02`
- **Type**: Hash
- **Fields**:
  - `session_count` - Total unique sessions
  - `session_seconds_total` - Total seconds across all sessions
  - `engaged_sessions` - Total engaged sessions
  - `visits` - Total page views (from track-visit)
  - `unique_users` - Unique users (from track-visit)
  - `new_users` - New users (from track-visit)
  - `returning_users` - Returning users (from track-visit)

## Integration

### Pages with Tracking

All tracking (both visit and session) is integrated into 11 pages total.

## Querying Analytics

To retrieve session analytics from Upstash:

```redis
HGETALL mates:analytics:day:2026-01-02
HGET mates:analytics:day:2026-01-02 session_count
HGETALL mates:analytics:day:2026-01-02:pathstats
```
