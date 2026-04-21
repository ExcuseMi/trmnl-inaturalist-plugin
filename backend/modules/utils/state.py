import asyncio
import json
import logging
import os
import random
import time

import asyncpg

log = logging.getLogger(__name__)

DATABASE_URL = os.getenv('DATABASE_URL', 'postgresql://postgres:postgres@localhost:5432/postgres')
MAX_OBSERVATIONS = max(200, int(os.getenv('OBSERVATIONS_PER_FETCH', '200')))

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
                    # Migrate existing fetch_log rows that predate the taxon/locale columns
                    await conn.execute("ALTER TABLE fetch_log ADD COLUMN IF NOT EXISTS taxon TEXT")
                    await conn.execute("ALTER TABLE fetch_log ADD COLUMN IF NOT EXISTS locale TEXT")
                    await conn.execute("""
                        CREATE TABLE IF NOT EXISTS instance_state (
                            key TEXT PRIMARY KEY,
                            state JSONB NOT NULL
                        )
                    """)
                    await conn.execute("""
                        CREATE TABLE IF NOT EXISTS geocode_cache (
                            key TEXT PRIMARY KEY,
                            location TEXT,
                            cached_at INTEGER NOT NULL
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


async def claim_fetch(query_key: str, interval_hours: int, taxon: str = '', locale: str = 'en') -> bool:
    """Atomically checks and claims the daily fetch slot. Returns True if this worker should fetch.

    Also registers the taxon/locale for this query_key so the background refresh
    can re-fetch it without needing an incoming request.
    """
    now = int(time.time())
    cutoff = now - interval_hours * 3600
    try:
        pool = await get_pool()
        async with pool.acquire() as conn:
            async with conn.transaction():
                # Always register taxon/locale so the background task can rediscover this key
                await conn.execute("""
                    INSERT INTO fetch_log (query_key, taxon, locale, last_fetched)
                    VALUES ($1, $2, $3, 0)
                    ON CONFLICT (query_key) DO UPDATE
                        SET taxon = EXCLUDED.taxon, locale = EXCLUDED.locale
                """, query_key, taxon or '', locale or 'en')

                # Atomically claim the slot if stale
                result = await conn.execute("""
                    UPDATE fetch_log SET last_fetched = $2
                    WHERE query_key = $1 AND last_fetched < $3
                """, query_key, now, cutoff)
                return result == 'UPDATE 1'
    except Exception as exc:
        log.warning('Could not claim fetch for %s: %s', query_key, exc)
    return True


async def get_known_queries() -> list[dict]:
    """Returns all taxon/locale combinations that have ever been requested."""
    try:
        pool = await get_pool()
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                'SELECT query_key, taxon, locale FROM fetch_log WHERE taxon IS NOT NULL AND locale IS NOT NULL'
            )
            return [dict(row) for row in rows]
    except Exception as exc:
        log.warning('Could not load known queries: %s', exc)
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


async def get_observation_ids(query_key: str) -> list[int]:
    try:
        pool = await get_pool()
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                'SELECT id FROM observations WHERE query_key = $1 ORDER BY fetched_at DESC, id DESC',
                query_key,
            )
            return [row['id'] for row in rows]
    except Exception as exc:
        log.warning('Could not load observation ids: %s', exc)
        return []


async def get_observation(obs_id: int, query_key: str) -> dict | None:
    try:
        pool = await get_pool()
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                'SELECT * FROM observations WHERE id = $1 AND query_key = $2',
                obs_id, query_key,
            )
            return dict(row) if row else None
    except Exception as exc:
        log.warning('Could not load observation %s: %s', obs_id, exc)
    return None


async def pick_observations(obs_ids: list[int], mode: str, key: str, count: int = 4) -> list[int]:
    if not obs_ids:
        return []

    if mode == 'random':
        return random.sample(obs_ids, min(count, len(obs_ids)))

    # Sequential: advance index by count each call
    state = await _load_state(key)
    idx = state.get('current_index', 0)
    if idx >= len(obs_ids):
        idx = 0

    selected = [obs_ids[(idx + i) % len(obs_ids)] for i in range(min(count, len(obs_ids)))]
    state['current_index'] = (idx + count) % len(obs_ids)
    await _save_state(key, state)
    return selected


async def _load_state(key: str) -> dict:
    try:
        pool = await get_pool()
        async with pool.acquire() as conn:
            row = await conn.fetchrow('SELECT state FROM instance_state WHERE key = $1', key)
            if row:
                v = row['state']
                return json.loads(v) if isinstance(v, str) else v
    except Exception as exc:
        log.warning('Could not load state for %s: %s', key, exc)
    return {'current_index': 0}


async def _save_state(key: str, state: dict):
    try:
        pool = await get_pool()
        async with pool.acquire() as conn:
            await conn.execute(
                'INSERT INTO instance_state (key, state) VALUES ($1, $2) '
                'ON CONFLICT (key) DO UPDATE SET state = EXCLUDED.state',
                key, json.dumps(state),
            )
    except Exception as exc:
        log.warning('Could not save state for %s: %s', key, exc)
