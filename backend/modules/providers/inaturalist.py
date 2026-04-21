import logging
import os

import aiohttp

log = logging.getLogger(__name__)

INAT_API = 'https://api.inaturalist.org/v1/observations'
PER_PAGE = 200  # iNaturalist API maximum
PHOTO_SIZE = 'large'


async def fetch_observations(taxon: str, locale: str = 'en') -> list[dict]:
    photo_licenses = os.getenv('PHOTO_LICENSES', 'cc-by,cc0').split(',')
    fetch_pages = max(1, int(os.getenv('OBSERVATIONS_PER_FETCH', '200')) // PER_PAGE)
    params: list[tuple] = [
        ('quality_grade', 'research'),
        ('captive', 'false'),
        ('popular', 'true'),
        ('photos', 'true'),
        ('per_page', PER_PAGE),
        ('order_by', 'votes'),
        ('order', 'desc'),
        ('locale', locale),
    ]
    for lic in photo_licenses:
        params.append(('photo_license', lic.strip()))
    if taxon:
        for t in taxon.split(','):
            params.append(('iconic_taxa', t.strip()))

    headers = {'User-Agent': 'TRMNL-iNaturalist-Plugin/1.0 (self-hosted)'}
    results: list[dict] = []
    seen_ids: set[int] = set()
    last_id: int | None = None

    async with aiohttp.ClientSession(headers=headers) as session:
        for _ in range(fetch_pages):
            page_params = list(params)
            if last_id is not None:
                page_params.append(('id_below', last_id))
            async with session.get(
                INAT_API,
                params=page_params,
                timeout=aiohttp.ClientTimeout(total=30),
            ) as resp:
                resp.raise_for_status()
                data = await resp.json()

            items = data.get('results', [])
            if not items:
                break

            for item in items:
                obs = _parse(item)
                if obs and obs['id'] not in seen_ids:
                    seen_ids.add(obs['id'])
                    results.append(obs)

            if len(items) < PER_PAGE:
                break

            last_id = min(item['id'] for item in items)

    log.info('Fetched %d observations from iNaturalist (taxon=%r locale=%s)', len(results), taxon or 'all', locale)
    return results


def _parse(item: dict) -> dict | None:
    photos = item.get('photos') or []
    if not photos:
        return None

    photo = photos[0]
    raw_url = photo.get('url', '')
    if not raw_url:
        return None

    photo_url = raw_url.replace('/square.', f'/{PHOTO_SIZE}.')

    taxon = item.get('taxon') or {}
    user = item.get('user') or {}

    gps_lat = gps_lon = None
    location = item.get('location')
    if location:
        try:
            lat_s, lon_s = location.split(',', 1)
            gps_lat = float(lat_s)
            gps_lon = float(lon_s)
        except (ValueError, AttributeError):
            pass

    return {
        'id': item['id'],
        'taxon_id': taxon.get('id'),
        'taxon_name': taxon.get('name'),
        'taxon_common_name': taxon.get('preferred_common_name'),
        'iconic_taxon_name': taxon.get('iconic_taxon_name'),
        'observed_on': item.get('observed_on'),
        'photo_url': photo_url,
        'photo_license': photo.get('license_code'),
        'photo_attribution': photo.get('attribution'),
        'observer_login': user.get('login'),
        'observer_name': user.get('name'),
        'place_guess': item.get('place_guess'),
        'gps_lat': gps_lat,
        'gps_lon': gps_lon,
    }
