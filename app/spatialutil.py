import os
import os.path as path
import geopandas as gpd
import pandas as pd
import shapely.geometry as geom
import shapely.speedups as fast # Really?
import matplotlib.pyplot as plt

# Need libspatialindex for sjoin
# brew install spatialindex
# pip install rtree

def spatial_join():
    df = pd.read_csv(r"../data/reprocessed/xyz/uuid_20190626_8bfee6d7ec345d3b503a4ed3adc0288b_pointData.xyz")
    df.drop_duplicates(['UUID', 'LON', 'LAT', 'DEPTH'], inplace=True)
    geometry = [geom.Point(xy) for xy in zip(df.LON, df.LAT)]
    crs = "epsg:4326"  # http://www.spatialreference.org/ref/epsg/4326/
    pointsGdf = gpd.GeoDataFrame(df, crs=crs, geometry=geometry)

fast.enable()
# Load CSB points

#df = pd.read_csv(r"../data/reprocessed/xyz/subsetDD.xyz")





# Read in the polygons used to filter CSB points
fp = r"../data/gis/TERRITORIAL_SEAS.shp"
dataGdf = gpd.read_file(fp)
#print("CRS: " + str(dataGdf.crs))

# Create a subset of with just country name and exclude columns
subGdf = dataGdf[['EXCLUDE', 'geometry']]

# Try spatial join
join = gpd.sjoin(pointsGdf, subGdf, how="left", op="within")
sharePts = join[join['EXCLUDE'] != "Y"]
#noSharePts = join[join['EXCLUDE'] == "Y"]

print(sharePts)
sharePts.to_file("../data/reprocessed/shapefiles/sharePts.shp")
if path.exists("../data/reprocessed/xyz/sharePoints.csv"):
    os.remove("../data/reprocessed/xyz/sharePoints.csv")

join.to_csv("../data/reprocessed/xyz/sharePoints.csv", index=False)

# Create a subset of polygons for just those areas we want to filter out
polyFilterGf = subGdf[subGdf['EXCLUDE'] == "Y"]

fig, ax = plt.subplots()
# Display all areas in gray
dataGdf.plot(ax=ax, facecolor='gray')

# Display excluded areas in red

polyFilterGf.plot(ax=ax, facecolor='red')

# Display points in exclusion area as yellow
#noSharePts.plot(ax=ax, color='yellow', markersize=7);

# Display points outside of exclusion area as green
sharePts.plot(ax=ax, color='green', markersize=7);
# Plot it
plt.show()

