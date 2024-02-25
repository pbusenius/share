import pandas as pd
import geopandas as gpd
import fast_geo_distance

from shapely.ops import nearest_points

WPI_POINT_BUFFER = 500
WPI_DISTANCE_THRESHOLD = 15_000
NEAREST_POINT_THRESHOLD = 2_000
DDPI_FILE = "data/ddpi_v2.geojson"
WORLD_FILE = "data/world.geojson"
COUNTRY_FILE = "data/custom.geojson"
WPI_FILE = "data/wpi.geojson"


def filter_not_in_ddpi(ddpi_gdf, wpi_gdf):
    negative_df = wpi_gdf.copy()

    for _, row in ddpi_gdf.iterrows():
        negative_df = negative_df[~negative_df.within(row.geometry)]

    return negative_df


def get_point_of_wpi_port(wpi_gdf, world_gdf):
    wpi_gdf["nearest_point"] = wpi_gdf.apply(lambda x: nearest_points(world_gdf.geometry.boundary, x.geometry)[0], axis=1)

    wpi_gdf["distance"] = wpi_gdf.apply(lambda x: fast_geo_distance.geodesic(x.Latitude, x.Longitude, x.nearest_point.y, x.nearest_point.x), axis=1)

    return wpi_gdf[wpi_gdf["distance"] <= WPI_DISTANCE_THRESHOLD]


def main():
    wpi_gdf = gpd.read_file(WPI_FILE)
    world_gdf = gpd.read_file(WORLD_FILE)
    ddpi_gdf = gpd.read_file(DDPI_FILE)

    ddpi_buffered_gdf = ddpi_gdf.copy()
    ddpi_buffered_gdf["geometry"] = ddpi_buffered_gdf["geometry"].buffer(WPI_DISTANCE_THRESHOLD / 111111)

    countries_df = gpd.read_file(COUNTRY_FILE)
    countries_df = countries_df[["adm0_a3", "geometry"]]
    countries_df.rename(columns={"adm0_a3": "country_code"}, inplace=True)

    id = ddpi_gdf["id"].max()
    id += 1

    wpi_no_in_ddpi_gdf = filter_not_in_ddpi(ddpi_buffered_gdf, wpi_gdf)
    wpi_no_in_ddpi_gdf.to_file("test.geojson")

    wpi_no_in_ddpi_gdf = get_point_of_wpi_port(wpi_no_in_ddpi_gdf, world_gdf)

    wpi_no_in_ddpi_gdf = gpd.GeoDataFrame(wpi_no_in_ddpi_gdf, geometry=wpi_no_in_ddpi_gdf.nearest_point)

    wpi_no_in_ddpi_gdf["geometry"] = wpi_no_in_ddpi_gdf["geometry"].buffer(WPI_POINT_BUFFER / 111111)
    wpi_no_in_ddpi_gdf["is_anchorage"] = False

    wpi_no_in_ddpi_gdf = wpi_no_in_ddpi_gdf.reset_index()
    wpi_no_in_ddpi_gdf["id"] = wpi_no_in_ddpi_gdf.index + id

    wpi_no_in_ddpi_gdf = gpd.sjoin(wpi_no_in_ddpi_gdf, countries_df, how="left", predicate="intersects")    

    wpi_no_in_ddpi_gdf = wpi_no_in_ddpi_gdf[["id", "is_anchorage", "geometry", "country_code"]]

    wpi_no_in_ddpi_gdf = pd.concat([ddpi_gdf, wpi_no_in_ddpi_gdf])

    wpi_no_in_ddpi_gdf.to_file("data/ddpi_v2.1.geojson")


if __name__ == "__main__":
    main()
