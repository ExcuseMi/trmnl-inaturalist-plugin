import asyncio
import hashlib
import logging
import os

from quart import Quart, jsonify, request

from modules.providers.inaturalist import fetch_observations, fetch_taxon_name
from modules.utils.geocode import reverse_geocode
from modules.utils.ip_whitelist import init_ip_whitelist, require_trmnl_ip
from modules.utils.state import (
    cache_taxon_name,
    claim_fetch,
    get_cached_taxon_name,
    get_known_queries,
    get_observation,
    get_observation_ids,
    init_db,
    pick_observations,
    store_observations,
)

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(name)s %(message)s')
log = logging.getLogger(__name__)

app = Quart(__name__)

FETCH_INTERVAL_HOURS = int(os.getenv('FETCH_INTERVAL_HOURS', '24'))

# All selectable categories — seeded on startup so every filter is warm before the first request
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

    qkey = _query_key(taxon)

    if await claim_fetch(qkey, FETCH_INTERVAL_HOURS, taxon):
        log.info('Fetching fresh observations key=%s taxon=%r', qkey, taxon)
        try:
            fresh = await fetch_observations(taxon)
            await store_observations(qkey, fresh)
        except Exception:
            log.exception('Failed to fetch observations from iNaturalist')

    obs_ids = await get_observation_ids(qkey)
    if not obs_ids:
        return jsonify(_error_response('No observations found.'))

    selected_ids = await pick_observations(obs_ids, 'random', qkey, count=4)
    if not selected_ids:
        return jsonify(_error_response('No observations available.'))

    items = []
    for obs_id in selected_ids:
        obs = await get_observation(obs_id, qkey)
        if not obs:
            continue

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

    log.info('Serving %d observations key=%s locale=%s taxon=%r', len(items), qkey, locale, taxon or 'all')
    return jsonify({
        'items': items,
        'total_count': len(obs_ids),
        'error': None,
    })


async def _resolve_taxon_name(taxon_id: int | None, locale: str, fallback: str | None) -> str | None:
    if not taxon_id or locale == 'en':
        return fallback
    cached = await get_cached_taxon_name(taxon_id, locale)
    if cached is not None:
        return cached
    name = await fetch_taxon_name(taxon_id, locale)
    if name:
        await cache_taxon_name(taxon_id, locale, name)
        return name
    return fallback


async def _background_refresh():
    """Proactively refreshes all taxon combinations once per FETCH_INTERVAL_HOURS.

    Covers the full SEED_TAXA list plus any custom multi-taxon combinations from real requests.
    Locale is no longer a fetch concern — common names are resolved at serve time.
    """
    await asyncio.sleep(10)
    try:
        while True:
            seen = set()
            queries = []
            for taxon in SEED_TAXA:
                key = _query_key(taxon)
                if key not in seen:
                    seen.add(key)
                    queries.append({'query_key': key, 'taxon': taxon})
            for q in await get_known_queries():
                if q['query_key'] not in seen:
                    seen.add(q['query_key'])
                    queries.append(q)

            log.info('Background refresh: checking %d queries', len(queries))
            for q in queries:
                taxon = q['taxon'] or ''
                if await claim_fetch(q['query_key'], FETCH_INTERVAL_HOURS, taxon):
                    log.info('Background refresh: fetching taxon=%r', taxon)
                    try:
                        fresh = await fetch_observations(taxon)
                        await store_observations(q['query_key'], fresh)
                    except Exception:
                        log.exception('Background refresh failed taxon=%r', taxon)
                await asyncio.sleep(2)
            await asyncio.sleep(FETCH_INTERVAL_HOURS * 3600)
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


def _query_key(taxon: str) -> str:
    return hashlib.sha256((taxon or 'all').encode()).hexdigest()[:16]


def _error_response(message: str) -> dict:
    log.warning('Returning error: %s', message)
    return {'observation': None, 'total_count': 0, 'error': message}


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080)
