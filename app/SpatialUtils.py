import geopandas as gpd
import pandas as pd
import shapely.geometry as geom
import shapely.speedups as fast # Really?
import matplotlib.pyplot as plt
fast.enable()
# Load CSB points
#df = pd.read_csv(r"../data/reprocessed/xyz/uuid_20190626_8bfee6d7ec345d3b503a4ed3adc0288b_pointData.xyz")
df = pd.read_csv(r"../data/reprocessed/xyz/subset.xyz")
df.drop_duplicates(['UUID', 'LON', 'LAT', 'DEPTH'], inplace=True)
geometry = [geom.Point(xy) for xy in zip(df.LON, df.LAT)]
crs = "epsg:4326" #http://www.spatialreference.org/ref/epsg/4326/
pointsGdf = gpd.GeoDataFrame(df, crs=crs, geometry=geometry)
print(pointsGdf)
# Read in the polygons used to filter CSB points
fp = r"../data/gis/TERRITORIAL_SEAS.shp"
dataGdf = gpd.read_file(fp)
print("CRS: " + str(dataGdf.crs))

# Create a subset of with just country name and exclude columns
subGdf = dataGdf[['SOVEREIGN1', 'EXCLUDE', 'geometry']].head(10)
# Create a subset of polygons for just those areas we want to filter out
polyFilterGf = subGdf[subGdf['EXCLUDE'] == "Y"]
# Reset indexes
polyFilterGf.reset_index(drop=True, inplace=True)
# Find all points NOT within exclusion areas
pipMask = dataGdf.within(polyFilterGf.loc[0, 'geometry'])
pnipMask = ~pipMask
#print(pnipMask)
pnipGdf = pointsGdf.loc[pnipMask]
pipGdf = pointsGdf.loc[pipMask]
fig, ax = plt.subplots()
print(pnipGdf)
# Display all areas in gray
dataGdf.plot(ax=ax, facecolor='gray')

# Display excluded areas in red
polyFilterGf.plot(ax=ax, facecolor='red')

# Display points in exclusion area as yellow
pipGdf.plot(ax=ax, color='yellow', markersize=7);
# Display points outside of exclusion area as green
pnipGdf.plot(ax=ax, color='green', markersize=7);
# Plot it
plt.show()

