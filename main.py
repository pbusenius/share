import h3
import json
import tqdm
import geopandas as gpd

from shapely.geometry import shape, Polygon
from shapely.ops import transform


def flip(x, y):
    """Flips the x and y coordinate values"""
    return y, x

def compute_polygon(polygon: Polygon):
    polygon = transform(flip, polygon)
    h3_cells = h3.polygon_to_cells(h3.Polygon(polygon.exterior.coords), 9)

    h3_cells = {cell: 0 for cell in h3_cells}

    inner_cells = []
    outer_cells = []
    
    for cell in h3_cells:
        grid_cells = [1  if i not in h3_cells else 0 for i in h3.grid_ring(cell, 1)]

        if sum(grid_cells) >= 1:
            outer_cells.append(cell)
        else:
            inner_cells.append(cell)
          
    return {"inner_cells": inner_cells, "outer_cells": outer_cells}

def get_city_name(polygon: Polygon, cities_df: gpd.GeoDataFrame):
    city = {}

    gdf = gpd.GeoDataFrame(index=[0], crs='epsg:4326', geometry=[polygon])

    city["city"] = cities_df.sjoin(gdf, how="right").drop_duplicates(subset=['NAME'])["NAME"].fillna("").tolist()

    return city


def get_country(polygon: Polygon, countries_df: gpd.GeoDataFrame):
    country = {}
    gdf = gpd.GeoDataFrame(index=[0], crs='epsg:4326', geometry=[polygon])
    country["country"] = countries_df.sjoin(gdf, how="right", predicate="intersects")["adm0_a3"].fillna("").tolist()

    return country

def main():
    cities_df = gpd.read_file("cities.geojson")
    cities_df = cities_df.to_crs(5070)

    cities_df['geometry'] = cities_df['geometry'].buffer(500)
    cities_df = cities_df.to_crs(4326)

    countries_df = gpd.read_file("countries.json")
    countries_df = countries_df[["adm0_a3", "geometry"]]    
    
    with open("ddpi.geojson") as f:
        data = json.load(f)

    for i in tqdm.tqdm(range(len(data["features"]))):
        data["features"][i]["properties"] = {}
        data["features"][i]["properties"]["h3_cells"] = compute_polygon(shape(data["features"][i]["geometry"]))
        data["features"][i]["properties"]["city"] = get_city_name(shape(data["features"][i]["geometry"]), cities_df)
        data["features"][i]["properties"]["country"] = get_country(shape(data["features"][i]["geometry"]), countries_df)

    with open("ddpi_v2.geojson", "w") as f:
        json.dump(data, f)



        
if __name__ == "__main__":
    main()