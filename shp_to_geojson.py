import os
import geopandas as gpd

shapefiles = [
    ("Data/tl_2022_13_cd118/tl_2022_13_cd118.shp", "Data/tl_2022_13_cd118.geojson"),
    ("Data/tl_2022_13_sldl/tl_2022_13_sldl.shp",   "Data/tl_2022_13_sldl.geojson"),
    ("Data/tl_2022_13_sldu/tl_2022_13_sldu.shp",   "Data/tl_2022_13_sldu.geojson"),
]

for shp, out in shapefiles:
    gdf = gpd.read_file(shp).to_crs("EPSG:4326")
    gdf.to_file(out, driver="GeoJSON")
    kb = round(os.path.getsize(out) / 1024, 1)
    print(f"  {out}  ({len(gdf)} features, {kb} KB)")

print("Done.")
