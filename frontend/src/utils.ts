/** Compute a bounding box around a point with a given radius in miles. */
export function getUserBbox(lat: number, lon: number, radiusMiles: number = 100): [number, number, number, number] {
  const radiusDeg = radiusMiles / 69.0;
  return [lon - radiusDeg, lat - radiusDeg, lon + radiusDeg, lat + radiusDeg];
}

/** Haversine distance between two points in miles. */
export function haversineDistance(lat1: number, lon1: number, lat2: number, lon2: number): number {
  const R = 3959;
  const dLat = (lat2 - lat1) * Math.PI / 180;
  const dLon = (lon2 - lon1) * Math.PI / 180;
  const a = Math.sin(dLat / 2) ** 2
    + Math.cos(lat1 * Math.PI / 180) * Math.cos(lat2 * Math.PI / 180) * Math.sin(dLon / 2) ** 2;
  return R * 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
}

/** Extract bounding box from a GeoJSON geometry. */
export function geometryToBbox(geom: any): [number, number, number, number] {
  const coords: number[][] = geom.coordinates[0];
  const lons = coords.map((c) => c[0]);
  const lats = coords.map((c) => c[1]);
  return [Math.min(...lons), Math.min(...lats), Math.max(...lons), Math.max(...lats)];
}

/** Convert snake_case region name to Title Case. */
export function formatRegionName(name: string): string {
  return name.replace(/_/g, ' ').replace(/\b\w/g, (c) => c.toUpperCase());
}
