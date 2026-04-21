import asyncio
import json
import logging
import os
import random
import time

import asyncpg

log = logging.getLogger(__name__)

DATABASE_URL = os.getenv('DATABASE_URL', 'postgresql://postgres:postgres@localhost:5432/postgres')
MAX_OBSERVATIONS = 1000  # keep 5 pages of 200 per query key

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
                        last_fetched INTEGER NOT NULL
                    )
                """)
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
            log.info('PostgreSQL initialized')
            break
        except Exception as e:
            if i == 9:
                log.error('Failed to connect to PostgreSQL after 10 attempts: %s', e)
                raise
            log.warning('PostgreSQL not ready, retrying in 2s... (%d/10)', i + 1)
            await asyncio.sleep(2)
    return _pool


async def should_fetch(query_key: str, interval_hours: int) -> bool:
    try:
        pool = await get_pool()
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                'SELECT last_fetched FROM fetch_log WHERE query_key = $1', query_key
            )
            if row:
                return (time.time() - row['last_fetched']) > interval_hours * 3600
    except Exception as exc:
        log.warning('Could not check fetch_log: %s', exc)
    return True


async def mark_fetched(query_key: str):
    try:
        pool = await get_pool()
        async with pool.acquire() as conn:
            await conn.execute(
                'INSERT INTO fetch_log (query_key, last_fetched) VALUES ($1, $2) '
                'ON CONFLICT (query_key) DO UPDATE SET last_fetched = EXCLUDED.last_fetched',
                query_key, int(time.time()),
            )
    except Exception as exc:
        log.warning('Could not update fetch_log: %s', exc)


async def store_observations(query_key: str, observations: list[dict]):
    if not observations:
        return
    now = int(time.time())
    pool = await get_pool()
    async with pool.acquire() as conn:
        for obs in observations:
            await conn.execute("""
                INSERT INTO observations (
                    id, query_key, taxon_id, taxon_name, taxon_common_name,
                    iconic_taxon_name, observed_on, photo_url, photo_license,
                    photo_attribution, observer_login, observer_name,
                    place_guess, gps_lat, gps_lon, fetched_at
                ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16)
                ON CONFLICT (id, query_key) DO UPDATE SET
                    photo_url = EXCLUDED.photo_url,
                    fetched_at = EXCLUDED.fetched_at
            """,
                obs['id'], query_key, obs.get('taxon_id'), obs.get('taxon_name'),
                obs.get('taxon_common_name'), obs.get('iconic_taxon_name'),
                obs.get('observed_on'), obs.get('photo_url'), obs.get('photo_license'),
                obs.get('photo_attribution'), obs.get('observer_login'), obs.get('observer_name'),
                obs.get('place_guess'), obs.get('gps_lat'), obs.get('gps_lon'), now,
            )

        # Keep the pool bounded per query_key
        await conn.execute("""
            DELETE FROM observations
            WHERE query_key = $1
              AND id NOT IN (
                SELECT id FROM observations
                WHERE query_key = $1
                ORDER BY fetched_at DESC
                LIMIT $2
              )
        """, query_key, MAX_OBSERVATIONS)

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


async def pick_observation(obs_ids: list[int], mode: str, key: str) -> int | None:
    if not obs_ids:
        return None

    if mode == 'random':
        return random.choice(obs_ids)

    # Sequential: cycle through via stored index
    state = await _load_state(key)
    idx = state.get('current_index', 0)
    if idx >= len(obs_ids):
        idx = 0

    selected = obs_ids[idx]
    state['current_index'] = (idx + 1) % len(obs_ids)
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
