import pandas as pd
import geopandas as gpd
import shapely.wkt
import shapely.ops

from shapely.ops import nearest_points


DDPI_FILE = "ddpi.geojson"
CITIES_FILE = "cities.json"
DISTANCE_FILE = "hub_line_distance.geojson"
DISTANCE_THRESHOLD = 20
DDPI_FILE_OUTPUT = "ddpi_v2.geojson"


def combine_overlapping_polygongs(ddpi_gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    ddpi_gdf = ddpi_gdf.sjoin(ddpi_gdf, how="left", predicate="intersects")
    ddpi_gdf = ddpi_gdf.dissolve("id_right")

    ddpi_gdf = ddpi_gdf.reset_index().dissolve("id_left")

    ddpi_gdf = ddpi_gdf.drop(["country_code_right", "id_right", "is_anchorage_right", "index_right"], axis=1).reset_index()

    return ddpi_gdf.rename(columns={"id_left": "id", "country_code_left": "country_code", "is_anchorage_left": "is_anchorage"})


def main():
    ddpi_gdf = gpd.read_file(DDPI_FILE)

    ddpi_gdf = combine_overlapping_polygongs(ddpi_gdf)

    ddpi_gdf.to_file(DDPI_FILE_OUTPUT)


if __name__ == "__main__":
    main()
