import logging

import aiohttp

log = logging.getLogger(__name__)

INAT_API = 'https://api.inaturalist.org/v1/observations'
PER_PAGE = 200   # iNaturalist API maximum
FETCH_PAGES = 5  # fetch up to 5 pages (1000 observations) per daily refresh
PHOTO_SIZE = 'large'


async def fetch_observations(
    taxon: str,
    lat: float | None,
    lng: float | None,
    radius_km: int | None,
    feed: str = 'popular',
) -> list[dict]:
    order_by = 'votes' if feed == 'popular' else 'created_at'
    base_params: dict = {
        'quality_grade': 'research',
        'photos': 'true',
        'photo_licensed': 'true',
        'per_page': PER_PAGE,
        'order_by': order_by,
        'order': 'desc',
    }
    if taxon:
        base_params['iconic_taxon_name'] = taxon
    if lat is not None and lng is not None and radius_km is not None:
        base_params['lat'] = lat
        base_params['lng'] = lng
        base_params['radius'] = radius_km

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

    log.info('Fetched %d observations from iNaturalist (taxon=%r feed=%s)', len(results), taxon or 'all', feed)
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
