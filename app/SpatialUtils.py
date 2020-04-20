import geopandas as gpd
import pandas as pd
import shapely.geometry as geom

import matplotlib.pyplot as plt

######################## Start Load points

#df = pd.read_csv(r"../data/reprocessed/xyz/uuid_20190626_8bfee6d7ec345d3b503a4ed3adc0288b_pointData.xyz")
df = pd.read_csv(r"../data/reprocessed/xyz/subset.xyz")

geometry = [geom.Point(xy) for xy in zip(df.LON, df.LAT)]
crs = "epsg:4326" #http://www.spatialreference.org/ref/epsg/4326/
pointsGdf = gpd.GeoDataFrame(df, crs=crs, geometry=geometry)
######################## End Load points

# Read in the polygons used to filter CSB points
fp = r"../data/gis/TERRITORIAL_SEAS.shp"
dataGf = gpd.read_file(fp)

print("CRS: " + str(dataGf.crs))

# Create a subset of with just country name and exclude columns
subDf = dataGf[['SOVEREIGN1', 'EXCLUDE', 'geometry']].head(10)
# Create a subset of polygons for just those areas we want to filter out
polyFilterGf = subDf[subDf['EXCLUDE'] == "Y"]
# Reset indexes
polyFilterGf.reset_index(drop=True, inplace=True)

print(subDf)

fig, ax = plt.subplots()

# Display all areas in gray
dataGf.plot(ax=ax, facecolor='gray')

# Display excluded areas in red
polyFilterGf.plot(ax=ax, facecolor='red')

# Display points in green
pointsGdf.plot(ax=ax, color='green', markersize=5);

# Plot it
plt.show()

