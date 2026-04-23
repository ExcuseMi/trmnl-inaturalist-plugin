import asyncio
import logging
import os
import time

import asyncpg

log = logging.getLogger(__name__)

DATABASE_URL = os.getenv('DATABASE_URL', 'postgresql://postgres:postgres@localhost:5432/postgres')
TAXON_NAME_CACHE_TTL = 7 * 24 * 3600  # 7 days

_pool = None


async def get_pool():
    global _pool
    if _pool is None:
        await init_db()
    return _pool


async def init_db():
    global _pool
    if _pool is not None:
        return _pool

    for i in range(10):
        try:
            _pool = await asyncpg.create_pool(DATABASE_URL)
            async with _pool.acquire() as conn:
                # Advisory lock ensures only one worker runs schema creation at a time
                await conn.execute("SELECT pg_advisory_lock(8317000)")
                try:
                    await conn.execute("""
                        CREATE TABLE IF NOT EXISTS observations (
                            id BIGINT NOT NULL,
                            query_key TEXT NOT NULL,
                            taxon_id INTEGER,
                            taxon_name TEXT,
                            taxon_common_name TEXT,
                            iconic_taxon_name TEXT,
                            observed_on TEXT,
                            photo_url TEXT,
                            photo_license TEXT,
                            photo_attribution TEXT,
                            observer_login TEXT,
                            observer_name TEXT,
                            place_guess TEXT,
                            gps_lat DOUBLE PRECISION,
                            gps_lon DOUBLE PRECISION,
                            fetched_at INTEGER NOT NULL,
                            PRIMARY KEY (id, query_key)
                        )
                    """)
                    await conn.execute("""
                        CREATE INDEX IF NOT EXISTS idx_obs_qkey_fetched
                        ON observations (query_key, fetched_at DESC)
                    """)
                    await conn.execute("""
                        CREATE TABLE IF NOT EXISTS fetch_log (
                            query_key TEXT PRIMARY KEY,
                            last_fetched INTEGER NOT NULL,
                            taxon TEXT,
                            locale TEXT
                        )
                    """)
                    await conn.execute("ALTER TABLE fetch_log ADD COLUMN IF NOT EXISTS taxon TEXT")
                    await conn.execute("ALTER TABLE fetch_log ADD COLUMN IF NOT EXISTS locale TEXT")
                    await conn.execute("""
                        CREATE TABLE IF NOT EXISTS geocode_cache (
                            key TEXT PRIMARY KEY,
                            location TEXT,
                            cached_at INTEGER NOT NULL
                        )
                    """)
                    await conn.execute("""
                        CREATE TABLE IF NOT EXISTS taxon_name_cache (
                            taxon_id INTEGER NOT NULL,
                            locale TEXT NOT NULL,
                            common_name TEXT,
                            cached_at INTEGER NOT NULL,
                            PRIMARY KEY (taxon_id, locale)
                        )
                    """)
                finally:
                    await conn.execute("SELECT pg_advisory_unlock(8317000)")
            log.info('PostgreSQL initialized')
            break
        except Exception as e:
            if i == 9:
                log.error('Failed to connect to PostgreSQL after 10 attempts: %s', e)
                raise
            log.warning('PostgreSQL not ready, retrying in 2s... (%d/10)', i + 1)
            await asyncio.sleep(2)
    return _pool


async def reset_fetch_claim(query_key: str):
    """Resets the fetch claim so the next request re-fetches (used after a failed fetch)."""
    try:
        pool = await get_pool()
        async with pool.acquire() as conn:
            await conn.execute(
                'UPDATE fetch_log SET last_fetched = 0 WHERE query_key = $1',
                query_key,
            )
    except Exception as exc:
        log.warning('Could not reset fetch claim for %s: %s', query_key, exc)


async def claim_fetch(query_key: str, interval_hours: int, taxon: str = '') -> bool:
    """Atomically claims the fetch slot if stale. Returns True if this worker should fetch."""
    now = int(time.time())
    cutoff = now - interval_hours * 3600
    try:
        pool = await get_pool()
        async with pool.acquire() as conn:
            async with conn.transaction():
                await conn.execute("""
                    INSERT INTO fetch_log (query_key, taxon, locale, last_fetched)
                    VALUES ($1, $2, '', 0)
                    ON CONFLICT (query_key) DO UPDATE SET taxon = EXCLUDED.taxon
                """, query_key, taxon or '')
                result = await conn.execute("""
                    UPDATE fetch_log SET last_fetched = $2
                    WHERE query_key = $1 AND last_fetched < $3
                """, query_key, now, cutoff)
                return result == 'UPDATE 1'
    except Exception as exc:
        log.warning('Could not claim fetch for %s: %s', query_key, exc)
    return True


async def get_observations(query_keys: list[str], count: int) -> list[dict]:
    """Returns a random sample of observations from one or more query keys."""
    try:
        pool = await get_pool()
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                'SELECT * FROM observations WHERE query_key = ANY($1) ORDER BY RANDOM() LIMIT $2',
                query_keys, count,
            )
            return [dict(row) for row in rows]
    except Exception as exc:
        log.warning('Could not load observations: %s', exc)
    return []


async def store_observations(query_key: str, observations: list[dict]):
    if not observations:
        return
    now = int(time.time())
    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.transaction():
            await conn.execute('DELETE FROM observations WHERE query_key = $1', query_key)
            for obs in observations:
                await conn.execute("""
                    INSERT INTO observations (
                        id, query_key, taxon_id, taxon_name, taxon_common_name,
                        iconic_taxon_name, observed_on, photo_url, photo_license,
                        photo_attribution, observer_login, observer_name,
                        place_guess, gps_lat, gps_lon, fetched_at
                    ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16)
                """,
                    obs['id'], query_key, obs.get('taxon_id'), obs.get('taxon_name'),
                    obs.get('taxon_common_name'), obs.get('iconic_taxon_name'),
                    obs.get('observed_on'), obs.get('photo_url'), obs.get('photo_license'),
                    obs.get('photo_attribution'), obs.get('observer_login'), obs.get('observer_name'),
                    obs.get('place_guess'), obs.get('gps_lat'), obs.get('gps_lon'), now,
                )
    log.info('Stored %d observations for key=%s', len(observations), query_key)


async def get_cached_taxon_name(taxon_id: int, locale: str) -> str | None:
    try:
        pool = await get_pool()
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                'SELECT common_name FROM taxon_name_cache WHERE taxon_id = $1 AND locale = $2 AND cached_at > $3',
                taxon_id, locale, int(time.time()) - TAXON_NAME_CACHE_TTL,
            )
            return row['common_name'] if row else None
    except Exception as exc:
        log.warning('Could not load taxon name cache %s/%s: %s', taxon_id, locale, exc)
    return None


async def cache_taxon_name(taxon_id: int, locale: str, common_name: str):
    try:
        pool = await get_pool()
        async with pool.acquire() as conn:
            await conn.execute(
                'INSERT INTO taxon_name_cache (taxon_id, locale, common_name, cached_at) VALUES ($1, $2, $3, $4) '
                'ON CONFLICT (taxon_id, locale) DO UPDATE SET common_name = EXCLUDED.common_name, cached_at = EXCLUDED.cached_at',
                taxon_id, locale, common_name, int(time.time()),
            )
    except Exception as exc:
        log.warning('Could not cache taxon name %s/%s: %s', taxon_id, locale, exc)
