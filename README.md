# trmnl-inaturalist-plugin

A [TRMNL](https://usetrmnl.com) plugin that displays research-grade wildlife observations from [iNaturalist](https://www.inaturalist.org) on your e-ink display.

## How it works

Once a day the backend fetches up to 1000 research-grade observations with photos from the iNaturalist API (5 pages × 200, no account required). These are stored in PostgreSQL. Each TRMNL device cycles through the pool independently — sequentially or at random.

The display shows the observation photo full-bleed with a configurable overlay: species name, scientific name, location, date observed, observer credit, and license.

## Self-hosting

**Requirements:** Docker + Docker Compose.

```bash
cp .env.example .env
# Edit .env if needed (port, DB credentials, IP whitelist)
docker compose up -d
```

The backend listens on port **8743** by default.

## TRMNL plugin settings

| Field | Description |
|---|---|

| **Category** | Taxon filter: Birds, Mammals, Insects, Plants, Fungi, Reptiles, Amphibians, Fish, Arachnids, Mollusks, or All |
| **Feed** | Popular (community-favourited, best photos) or Recent (newest observations) |
| **Filter by Location** | Restrict to observations near a lat/lng point |
| **Latitude / Longitude** | Center of the location search |
| **Search Radius** | 10 / 25 / 50 / 100 / 250 km |
| **Selection Mode** | Sequential (cycle in order) or Random |
| **Overlay Information** | Pick any combination of: common name, scientific name, location, observer, date, license |

## Database

PostgreSQL stores observation metadata only — no images. Each unique filter combination keeps at most 1000 observations; older ones are pruned automatically on each daily fetch.

## Environment variables

| Variable | Default | Description |
|---|---|---|
| `BACKEND_PORT` | `8743` | Host port |
| `FETCH_INTERVAL_HOURS` | `24` | How often to refresh observations |
| `ENABLE_IP_WHITELIST` | `true` | Restrict to TRMNL IP ranges |
| `IP_REFRESH_HOURS` | `24` | How often to refresh the TRMNL IP list |
| `POSTGRES_USER/PASSWORD/DB` | `postgres` | Database credentials |
