import asyncio
import hashlib
import logging
import os
from datetime import datetime, timedelta, timezone

from quart import Quart, jsonify, request

from modules.providers.inaturalist import fetch_observations, fetch_taxon_name
from modules.utils.geocode import reverse_geocode
from modules.utils.ip_whitelist import init_ip_whitelist, require_trmnl_ip
from modules.utils.state import (
    cache_taxon_name,
    claim_fetch,
    get_cached_taxon_name,
    get_observations,
    get_pool,
    init_db,
    reset_fetch_claim,
    store_observations,
)

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(name)s %(message)s')
log = logging.getLogger(__name__)

app = Quart(__name__)

FETCH_INTERVAL_HOURS = int(os.getenv('FETCH_INTERVAL_HOURS', '24'))
REFRESH_UTC_HOUR = int(os.getenv('REFRESH_UTC_HOUR', '1'))

SEED_TAXA = [
    'Aves', 'Mammalia', 'Insecta', 'Plantae', 'Fungi',
    'Reptilia', 'Amphibia', 'Actinopterygii', 'Arachnida', 'Mollusca',
]


@app.before_serving
async def _startup():
    await asyncio.gather(
        init_db(),
        init_ip_whitelist(),
    )
    asyncio.create_task(_background_refresh())


@app.route('/health')
async def health():
    return jsonify({'ok': True})


@app.route('/observation', methods=['GET', 'POST'])
@require_trmnl_ip
async def observation():
    body = await request.get_json(silent=True, force=True) or {}
    taxon = _parse_taxon(body.get('taxon'))
    locale = _normalize_locale(body.get('locale', 'en'))

    sort = body.get('sort', 'recent') if body.get('sort') in ('recent', 'votes') else 'recent'
    taxon_list = [t.strip() for t in taxon.split(',') if t.strip()] if taxon else []

    # Resolve query keys: multi-taxon composes from individual caches (OR), single uses its own key
    if len(taxon_list) > 1:
        query_keys = [_query_key(t, sort) for t in taxon_list]
        log.info('Composing from DB cache taxon=%r sort=%s keys=%s', taxon, sort, query_keys)
    else:
        qkey = _query_key(taxon, sort)
        claimed = await claim_fetch(qkey, FETCH_INTERVAL_HOURS, taxon)
        if not claimed:
            log.info('Cache hit — serving from DB taxon=%r sort=%s', taxon or 'all', sort)
            if not await get_observations([qkey], count=1):
                log.warning('Cache hit but DB empty for %r sort=%s — forcing re-fetch', taxon or 'all', sort)
                await reset_fetch_claim(qkey)
                claimed = True
        if claimed:
            log.info('Cache miss — fetching from iNaturalist taxon=%r sort=%s', taxon or 'all', sort)
            try:
                fresh = await fetch_observations(taxon, sort)
                await store_observations(qkey, fresh)
            except Exception:
                log.exception('iNaturalist fetch failed taxon=%r sort=%s', taxon or 'all', sort)
                await reset_fetch_claim(qkey)
        query_keys = [qkey]

    selected_obs = await get_observations(query_keys, count=4)
    if not selected_obs:
        return jsonify(_error_response('No observations found.'))

    items = []
    for obs in selected_obs:
        common_name = await _resolve_taxon_name(obs.get('taxon_id'), locale, obs.get('taxon_common_name'))
        location_name = None
        if obs.get('gps_lat') is not None and obs.get('gps_lon') is not None:
            try:
                location_name = await reverse_geocode(obs['gps_lat'], obs['gps_lon'], locale)
            except Exception:
                pass
        if not location_name:
            location_name = obs.get('place_guess')
        items.append({**obs, 'taxon_common_name': common_name, 'location_name': location_name})

    log.info('Served %d observations from DB locale=%s taxon=%r sort=%s', len(items), locale, taxon or 'all', sort)
    return jsonify({'items': items, 'total_count': len(items), 'error': None})


async def _resolve_taxon_name(taxon_id: int | None, locale: str, fallback: str | None) -> str | None:
    if not taxon_id or locale == 'en':
        return fallback
    cached = await get_cached_taxon_name(taxon_id, locale)
    if cached is not None:
        log.debug('Taxon name cache hit taxon_id=%s locale=%s name=%r', taxon_id, locale, cached)
        return cached
    name = await fetch_taxon_name(taxon_id, locale)
    if name:
        log.info('Taxon name fetched from iNaturalist taxon_id=%s locale=%s name=%r', taxon_id, locale, name)
        await cache_taxon_name(taxon_id, locale, name)
        return name
    log.debug('Taxon name not found taxon_id=%s locale=%s, using English fallback', taxon_id, locale)
    return fallback


async def _background_refresh():
    await asyncio.sleep(10)
    pool = await get_pool()
    async with pool.acquire() as _lock_conn:
        if not await _lock_conn.fetchval('SELECT pg_try_advisory_lock(8317001)'):
            log.info('Background refresh: another worker holds the lock, skipping')
            return
        try:
            await _do_background_refresh()
        finally:
            await _lock_conn.execute('SELECT pg_advisory_unlock(8317001)')


async def _do_background_refresh():
    """Refreshes all SEED_TAXA at REFRESH_UTC_HOUR daily.

    Runs immediately on startup so a missed scheduled window is caught,
    then sleeps until the next scheduled UTC hour.
    """
    try:
        while True:
            queries = [
                {'query_key': _query_key(t, s), 'taxon': t, 'sort': s}
                for t in SEED_TAXA
                for s in ('recent', 'votes')
            ]
            log.info('Background refresh: checking %d queries', len(queries))
            for q in queries:
                if await claim_fetch(q['query_key'], FETCH_INTERVAL_HOURS, q['taxon']):
                    log.info('Background refresh: fetching from iNaturalist taxon=%r sort=%s', q['taxon'], q['sort'])
                    try:
                        fresh = await fetch_observations(q['taxon'], q['sort'])
                        await store_observations(q['query_key'], fresh)
                    except Exception:
                        log.exception('Background refresh: iNaturalist fetch failed taxon=%r sort=%s', q['taxon'], q['sort'])
                        await reset_fetch_claim(q['query_key'])
                await asyncio.sleep(2)

            now = datetime.now(timezone.utc)
            next_run = now.replace(hour=REFRESH_UTC_HOUR, minute=0, second=0, microsecond=0)
            if next_run <= now:
                next_run += timedelta(days=1)
            sleep_seconds = (next_run - now).total_seconds()
            log.info('Background refresh: next run at %s UTC (in %.0fs)', next_run.strftime('%H:%M'), sleep_seconds)
            await asyncio.sleep(sleep_seconds)
    except asyncio.CancelledError:
        log.info('Background refresh task stopped')
        raise


def _normalize_locale(raw) -> str:
    if not raw or not isinstance(raw, str):
        return 'en'
    code = raw.strip().split('-')[0].split('_')[0].lower()
    return code or 'en'


def _parse_taxon(raw) -> str:
    if isinstance(raw, list):
        values = [v.strip() for v in raw if v and v.strip()]
    elif isinstance(raw, str) and raw.strip():
        values = [v.strip() for v in raw.strip().strip('[]').replace('"', '').split(',') if v.strip()]
    else:
        values = []
    return ','.join(sorted(values))


def _query_key(taxon: str, sort: str = 'recent') -> str:
    return hashlib.sha256(f"{taxon or 'all'}|{sort}".encode()).hexdigest()[:16]


def _error_response(message: str) -> dict:
    log.warning('Returning error: %s', message)
    return {'observation': None, 'total_count': 0, 'error': message}


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080)
