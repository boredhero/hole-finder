"""Zip code geocoding proxy — avoids CORS issues with external API."""

import httpx
from fastapi import APIRouter, HTTPException, Query

router = APIRouter(tags=["geocode"])

ZIPPOPOTAM_URL = "https://api.zippopotam.us/us"


@router.get("/geocode")
async def geocode_zip(
    zip: str = Query(..., min_length=5, max_length=5, pattern=r"^\d{5}$"),
):
    """Geocode a US zip code via Zippopotam.us.

    Returns lat/lon coordinates for the zip code centroid.
    Free API, no auth required, no rate limits.
    """
    async with httpx.AsyncClient(timeout=10.0) as client:
        try:
            resp = await client.get(f"{ZIPPOPOTAM_URL}/{zip}")
        except httpx.HTTPError:
            raise HTTPException(status_code=502, detail="Geocoding service unavailable")

    if resp.status_code == 404:
        raise HTTPException(status_code=404, detail="Invalid or unrecognized zip code")
    if resp.status_code != 200:
        raise HTTPException(status_code=502, detail="Geocoding service error")

    data = resp.json()
    places = data.get("places", [])
    if not places:
        raise HTTPException(status_code=404, detail="Invalid or unrecognized zip code")

    place = places[0]
    return {
        "lat": float(place["latitude"]),
        "lon": float(place["longitude"]),
        "city": place.get("place name", ""),
        "state": place.get("state abbreviation", ""),
    }
