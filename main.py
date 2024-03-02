import glob
import h3pandas
import geopandas as gpd
import polars as pl


H3_RESOLUTION = 11
DDPI_FILE = "ddpi_v2.1.geojson"
GLOB_PATTERN = "data/*.parquet"


def read_ddpi_file():
    return gpd.read_file(DDPI_FILE)


def main():
    files = glob.glob(GLOB_PATTERN)
    ddpi_gdf = read_ddpi_file()
    ddpi_gdf = ddpi_gdf.h3.polyfill(H3_RESOLUTION, explode=True)
    ddpi_gdf["h3_polyfill"] = ddpi_gdf["h3_polyfill"].apply(lambda x: int("0x"+x, 0))
    ddpi_gdf = ddpi_gdf[["id", "h3_polyfill"]]
    ddpi_gdf.rename(columns = {"id": "port_id", "h3_polyfill": "h3_cell"}, inplace = True)
    
    ddpi_df = pl.from_pandas(ddpi_gdf, schema_overrides={"port_id": pl.Int64, "h3_cell": pl.UInt64})

    combined = None
    for i in range(len(files)):
        df = (
            pl.read_parquet(files[i])
            .join(ddpi_df, on="h3_cell")
            .filter(pl.col("SOG")==0)
            .sort("TIMESTAMPUTC")
            .group_by_dynamic("TIMESTAMPUTC", by="MMSI", every="5d", closed="right")
            .agg([pl.col("LONGITUDE").first(),
                  pl.col("LATITUDE").first(),
                  pl.col("SHIPANDCARGOTYPECODE").first(),
                  pl.col("TIMESTAMPUTC").first().alias("TIMESTAMPUTC_PORTCALL"),
                  pl.col("port_id").first()])
            .with_columns(pl.col("port_id"))
        )

        if combined is None:
            combined = df
        else:
            combined.extend(df)


    combined = combined.sort([
        "MMSI",
        "TIMESTAMPUTC_PORTCALL"
    ]).with_columns(
        pl.col("port_id").shift().over("MMSI").alias("prev_port_id")
    ).filter(
        pl.col("port_id") != pl.col("prev_port_id")
    ).drop(["TIMESTAMPUTC"])

    combined.write_csv(f"combined.csv")


if __name__ == "__main__":
    main()
