import geopandas as gpd
import pandas as pd
import shapely.geometry as geom


# Need libspatialindex.so for sjoin and rtree python library
# brew install spatialindex
# pip install rtree

# Add attributes of polygons to points file
def spatial_join(csb_crawler, pts_file):
    # Read CSV file into a dataframe
    df = pd.read_csv(pts_file)

    poly_file = csb_crawler.test_data_dir + "gis/TERRITORIAL_SEAS.shp"
    # Read shapefile into geodataframe
    data_gdf = gpd.read_file(poly_file)

    # Remove dups and spatially enable points
    df.drop_duplicates(['UUID', 'LON', 'LAT', 'DEPTH','PLATFORM_NAME','PROVIDER'], inplace=True)
    geometry = [geom.Point(xy) for xy in zip(df.LON, df.LAT)]
    crs = "epsg:4326"  # http://www.spatialreference.org/ref/epsg/4326/
    points_gdf = gpd.GeoDataFrame(df, crs=crs, geometry=geometry)

    # Create a subset of with just country name and exclude columns
    sub_gdf = data_gdf[['EXCLUDE', 'geometry']]

    # Try spatial join
    join = gpd.sjoin(points_gdf, sub_gdf, how="left", op="within")
    return join
