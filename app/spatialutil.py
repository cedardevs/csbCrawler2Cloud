import os
import os.path as path
import geopandas as gpd
import pandas as pd
import shapely.geometry as geom
import shapely.speedups as fast # Really?
import matplotlib.pyplot as plt

# Need libspatialindex.so for sjoin and rtree python library
# brew install spatialindex
# pip install rtree

# Add attributes of polygons to points file
def spatial_join(crawler, pts_file, poly_file):
    #df = pd.read_csv(r"../data/reprocessed/xyz/uuid_20190626_8bfee6d7ec345d3b503a4ed3adc0288b_pointData.xyz")
    #fp = r"../data/gis/TERRITORIAL_SEAS.shp"

    # Read CSV file into a dataframe
    df = pd.read_csv(pts_file)

    # Read shapefile into geodataframe
    data_gdf = gpd.read_file(poly_file)

    # Remove dups and spatially enable points
    df.drop_duplicates(['UUID', 'LON', 'LAT', 'DEPTH'], inplace=True)
    geometry = [geom.Point(xy) for xy in zip(df.LON, df.LAT)]
    crs = "epsg:4326"  # http://www.spatialreference.org/ref/epsg/4326/
    points_gdf = gpd.GeoDataFrame(df, crs=crs, geometry=geometry)

    # Needed? fast.enable()


    # Create a subset of with just country name and exclude columns
    sub_gdf = data_gdf[['EXCLUDE', 'geometry']]

    # Try spatial join
    join = gpd.sjoin(points_gdf, sub_gdf, how="left", op="within")
    share_pts = join[join['EXCLUDE'] != "Y"]


    if path.exists("../data/reprocessed/xyz/sharePoints.csv"):
        os.remove("../data/reprocessed/xyz/sharePoints.csv")

    share_pts.to_csv("../data/reprocessed/xyz/share_points.csv", index=False)

