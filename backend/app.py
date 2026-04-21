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


@app.before_serving
async def _startup():
    await asyncio.gather(
        init_db(),
        init_ip_whitelist(),
    )


@app.route('/health')
async def health():
    return jsonify({'ok': True})


@app.route('/observation', methods=['GET', 'POST'])
@require_trmnl_ip
async def observation():
    body = await request.get_json(silent=True, force=True) or {}
    plugin_setting_id = str(body.get('plugin_setting_id', ''))
    taxon = body.get('taxon', '').strip()
    feed = body.get('feed', 'popular') if body.get('feed') in ('popular', 'recent') else 'popular'
    use_location = str(body.get('use_location', 'false')).lower() == 'true'
    mode = body.get('mode', 'sequential')

    units = body.get('units', 'metric')
    lat = lng = None
    radius_km = 50
    if use_location:
        try:
            lat = float(body.get('lat') or 0)
            lng = float(body.get('lng') or 0)
            radius = int(body.get('radius') or 50)
            radius_km = round(radius * 1.60934) if units == 'imperial' else radius
            if lat == 0 and lng == 0:
                lat = lng = None
        except (TypeError, ValueError):
            lat = lng = None

    qkey = _query_key(taxon, feed, lat, lng, radius_km if lat is not None else None)
    inst_key = plugin_setting_id or qkey

    if await claim_fetch(qkey, FETCH_INTERVAL_HOURS):
        log.info('Fetching fresh observations for key=%s taxon=%r feed=%s lat=%s lng=%s', qkey, taxon, feed, lat, lng)
        try:
            fresh = await fetch_observations(taxon, lat, lng, radius_km if lat is not None else None, feed)
            await store_observations(qkey, fresh)
        except Exception:
            log.exception('Failed to fetch observations from iNaturalist')

    obs_ids = await get_observation_ids(qkey)
    if not obs_ids:
        return jsonify(_error_response('No observations found. Try a broader taxon or location radius.'))

    selected_ids = await pick_observations(obs_ids, mode, inst_key, count=4)
    if not selected_ids:
        return jsonify(_error_response('No observations available.'))

    items = []
    for obs_id in selected_ids:
        obs = await get_observation(obs_id, qkey)
        if not obs:
            continue
        location_name = obs.get('place_guess')
        if not location_name and obs.get('gps_lat') is not None and obs.get('gps_lon') is not None:
            try:
                location_name = await reverse_geocode(obs['gps_lat'], obs['gps_lon'])
            except Exception:
                pass
        items.append({**obs, 'location_name': location_name})

    log.info('Serving %d observations mode=%s key=%s', len(items), mode, qkey)
    return jsonify({
        'items': items,
        'total_count': len(obs_ids),
        'error': None,
    })


def _query_key(taxon: str, feed: str, lat, lng, radius_km) -> str:
    parts = [taxon or 'all', feed]
    if lat is not None and lng is not None:
        parts += [f'{lat:.3f}', f'{lng:.3f}', str(radius_km)]
    return hashlib.sha256('|'.join(parts).encode()).hexdigest()[:16]


def _error_response(message: str) -> dict:
    log.warning('Returning error: %s', message)
    return {'observation': None, 'total_count': 0, 'error': message}


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080)
