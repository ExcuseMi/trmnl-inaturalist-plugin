import logging
import time

import aiohttp
from modules.utils.state import get_pool

log = logging.getLogger(__name__)

GEOCODE_TTL = 30 * 24 * 3600  # 30 days
_UA = 'TRMNL-iNaturalist-Plugin/1.0 (self-hosted)'


def _key(lat: float, lon: float) -> str:
    return f"{lat:.2f},{lon:.2f}"


async def forward_geocode(address: str) -> tuple[float, float] | None:
    key = f"q:{address.lower().strip()}"

    try:
        pool = await get_pool()
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                'SELECT location, cached_at FROM geocode_cache WHERE key = $1', key
            )
            if row and (time.time() - row['cached_at']) < GEOCODE_TTL:
                if row['location']:
                    lat_s, lon_s = row['location'].split(',', 1)
                    return float(lat_s), float(lon_s)
                return None
    except Exception as exc:
        log.warning('Forward geocode cache read failed: %s', exc)

    result = None
    try:
        url = 'https://nominatim.openstreetmap.org/search'
        params = {'q': address, 'format': 'json', 'limit': 1}
        async with aiohttp.ClientSession() as session:
            async with session.get(
                url,
                params=params,
                headers={'User-Agent': _UA},
                timeout=aiohttp.ClientTimeout(total=5),
            ) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    if data:
                        result = (float(data[0]['lat']), float(data[0]['lon']))
    except Exception as exc:
        log.warning('Nominatim forward lookup failed for %r: %s', address, exc)

    try:
        pool = await get_pool()
        async with pool.acquire() as conn:
            cached_val = f"{result[0]},{result[1]}" if result else None
            await conn.execute(
                'INSERT INTO geocode_cache (key, location, cached_at) VALUES ($1, $2, $3) '
                'ON CONFLICT (key) DO UPDATE SET location = EXCLUDED.location, cached_at = EXCLUDED.cached_at',
                key, cached_val, int(time.time()),
            )
    except Exception as exc:
        log.warning('Forward geocode cache write failed: %s', exc)

    return result


async def reverse_geocode(lat: float, lon: float) -> str | None:
    key = _key(lat, lon)

    try:
        pool = await get_pool()
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                'SELECT location, cached_at FROM geocode_cache WHERE key = $1', key
            )
            if row and (time.time() - row['cached_at']) < GEOCODE_TTL:
                return row['location'] or None
    except Exception as exc:
        log.warning('Geocode cache read failed: %s', exc)

    location = None
    try:
        url = (
            f"https://nominatim.openstreetmap.org/reverse"
            f"?lat={lat}&lon={lon}&format=json&zoom=10"
        )
        async with aiohttp.ClientSession() as session:
            async with session.get(
                url,
                headers={'User-Agent': 'TRMNL-iNaturalist-Plugin/1.0 (self-hosted)'},
                timeout=aiohttp.ClientTimeout(total=5),
            ) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    addr = data.get('address', {})
                    location = (
                        addr.get('city')
                        or addr.get('town')
                        or addr.get('village')
                        or addr.get('municipality')
                        or addr.get('county')
                        or addr.get('state')
                        or addr.get('country')
                    )
    except Exception as exc:
        log.warning('Nominatim lookup failed for %.4f,%.4f: %s', lat, lon, exc)

    try:
        pool = await get_pool()
        async with pool.acquire() as conn:
            await conn.execute(
                'INSERT INTO geocode_cache (key, location, cached_at) VALUES ($1, $2, $3) '
                'ON CONFLICT (key) DO UPDATE SET location = EXCLUDED.location, cached_at = EXCLUDED.cached_at',
                key, location, int(time.time()),
            )
    except Exception as exc:
        log.warning('Geocode cache write failed: %s', exc)

    return location
