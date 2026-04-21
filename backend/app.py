import asyncio
import hashlib
import logging
import os

from quart import Quart, jsonify, request

from modules.providers.inaturalist import fetch_observations
from modules.utils.geocode import reverse_geocode
from modules.utils.ip_whitelist import init_ip_whitelist, require_trmnl_ip
from modules.utils.state import (
    claim_fetch,
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
TOP_LOCALES = os.getenv('PRECACHE_LOCALES', 'en,es,fr,de,pt,nl,it,ja,zh,ko').split(',')


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

    qkey = _query_key(taxon, locale)
    en_qkey = _query_key(taxon, 'en')
    inst_key = qkey

    if await claim_fetch(qkey, FETCH_INTERVAL_HOURS, taxon, locale):
        log.info('Fetching fresh observations key=%s taxon=%r locale=%s', qkey, taxon, locale)
        try:
            fresh = await fetch_observations(taxon, locale)
            await store_observations(qkey, fresh)
            asyncio.create_task(_precache_locales(taxon, skip=locale))
        except Exception:
            log.exception('Failed to fetch observations from iNaturalist')

    obs_ids = await get_observation_ids(qkey)
    if not obs_ids and locale != 'en':
        log.info('No observations for locale=%s taxon=%r, falling back to en', locale, taxon)
        qkey = en_qkey
        if await claim_fetch(en_qkey, FETCH_INTERVAL_HOURS, taxon, 'en'):
            try:
                fresh = await fetch_observations(taxon, 'en')
                await store_observations(en_qkey, fresh)
            except Exception:
                log.exception('Failed to fetch English fallback observations')
        obs_ids = await get_observation_ids(en_qkey)

    if not obs_ids:
        return jsonify(_error_response('No observations found.'))

    selected_ids = await pick_observations(obs_ids, 'random', inst_key, count=4)
    if not selected_ids:
        return jsonify(_error_response('No observations available.'))

    items = []
    for obs_id in selected_ids:
        obs = await get_observation(obs_id, qkey)
        if not obs:
            continue
        location_name = None
        if obs.get('gps_lat') is not None and obs.get('gps_lon') is not None:
            try:
                location_name = await reverse_geocode(obs['gps_lat'], obs['gps_lon'], locale)
            except Exception:
                pass
        if not location_name:
            location_name = obs.get('place_guess')
        items.append({**obs, 'location_name': location_name})

    log.info('Serving %d observations key=%s locale=%s taxon=%r', len(items), qkey, locale, taxon or 'all')
    return jsonify({
        'items': items,
        'total_count': len(obs_ids),
        'error': None,
    })


async def _background_refresh():
    """Proactively refreshes all known taxon/locale combinations once per FETCH_INTERVAL_HOURS.

    Runs immediately on startup so stale caches are warmed before the first request of the day,
    then sleeps for the full interval before running again.
    """
    # Brief delay to let the server finish starting up
    await asyncio.sleep(10)
    try:
        while True:
            queries = await get_known_queries()
            if not queries:
                # Fresh install or reset — seed the default so the first request is served from cache
                queries = [{'query_key': _query_key('', 'en'), 'taxon': '', 'locale': 'en'}]
                log.info('Background refresh: no known queries, seeding default (all taxa, en)')
            else:
                log.info('Background refresh: checking %d known queries', len(queries))
                for q in queries:
                    taxon = q['taxon'] or ''
                    locale = q['locale'] or 'en'
                    if await claim_fetch(q['query_key'], FETCH_INTERVAL_HOURS, taxon, locale):
                        log.info('Background refresh: fetching taxon=%r locale=%s', taxon, locale)
                        try:
                            fresh = await fetch_observations(taxon, locale)
                            await store_observations(q['query_key'], fresh)
                        except Exception:
                            log.exception('Background refresh failed taxon=%r locale=%s', taxon, locale)
                    await asyncio.sleep(2)  # stagger requests to iNaturalist
            await asyncio.sleep(FETCH_INTERVAL_HOURS * 3600)
    except asyncio.CancelledError:
        log.info('Background refresh task stopped')
        raise


async def _precache_locales(taxon: str, skip: str):
    for locale in TOP_LOCALES:
        if locale == skip:
            continue
        qkey = _query_key(taxon, locale)
        if not await claim_fetch(qkey, FETCH_INTERVAL_HOURS, taxon, locale):
            continue
        try:
            fresh = await fetch_observations(taxon, locale)
            await store_observations(qkey, fresh)
            log.info('Pre-cached locale=%s taxon=%r', locale, taxon)
        except Exception:
            log.warning('Pre-cache failed locale=%s taxon=%r', locale, taxon)
        await asyncio.sleep(2)


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


def _query_key(taxon: str, locale: str = 'en') -> str:
    return hashlib.sha256(f"{taxon or 'all'}|{locale}".encode()).hexdigest()[:16]


def _error_response(message: str) -> dict:
    log.warning('Returning error: %s', message)
    return {'observation': None, 'total_count': 0, 'error': message}


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080)
