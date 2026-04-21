import logging
import os

import aiohttp

log = logging.getLogger(__name__)

INAT_API = 'https://api.inaturalist.org/v1/observations'
PER_PAGE = 200   # iNaturalist API maximum
FETCH_PAGES = 2  # fetch up to 2 pages (400 observations) per daily refresh
PHOTO_SIZE = 'large'


async def fetch_observations(taxon: str, locale: str = 'en') -> list[dict]:
    photo_licenses = os.getenv('PHOTO_LICENSES', 'cc-by,cc0')
    base_params: dict = {
        'quality_grade': 'research',
        'photos': 'true',
        'photo_license': photo_licenses,
        'per_page': PER_PAGE,
        'order_by': 'votes',
        'order': 'desc',
        'locale': locale,
    }
    if taxon:
        base_params['iconic_taxon_name'] = taxon

    headers = {'User-Agent': 'TRMNL-iNaturalist-Plugin/1.0 (self-hosted)'}
    results: list[dict] = []
    seen_ids: set[int] = set()

    async with aiohttp.ClientSession(headers=headers) as session:
        for page in range(1, FETCH_PAGES + 1):
            params = {**base_params, 'page': page}
            async with session.get(
                INAT_API,
                params=params,
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

            # Stop early if we got fewer results than requested
            if len(items) < PER_PAGE:
                break

    log.info('Fetched %d observations from iNaturalist (taxon=%r)', len(results), taxon or 'all')
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
