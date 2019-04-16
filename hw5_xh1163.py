from pyspark import SparkContext

def createIndex(geofile):
    import rtree
    import fiona.crs
    import geopandas as gpd
    zones = gpd.read_file(geofile).to_crs(fiona.crs.from_epsg(2263))
    index = rtree.Rtree()
    for idx,geometry in enumerate(zones.geometry):
        index.insert(idx, geometry.bounds)
    return (index, zones)

def findZone(p, index, zones):
    match = index.intersection((p.x, p.y, p.x, p.y))
    for idx in match:
        if zones.geometry[idx].contains(p):
            return idx
    return None

def processTrips(pid, records):
    import csv

    if pid==0:
         next(records)
    reader = csv.reader(records)

    import pyproj
    import shapely.geometry as geom
    proj = pyproj.Proj(init="epsg:2263", preserve_units=True)

    index_nei, neighborhoods = createIndex('neighborhoods.geojson')
    for row in reader:
        try:
            p = geom.Point(proj(float(row[9]), float(row[10])))
            found_b = findZone(p, index_nei, neighborhoods)
            boro =neighborhoods.borough[found_b]
            p1 = geom.Point(proj(float(row[5]), float(row[6])))
            found_n = findZone(p1, index_nei, neighborhoods)
            if found_n:
                neib = neighborhoods.neighborhood[found_n]
                yield ((boro, neib), 1)
        except:
            continue


def top_three(x):
    sorted_neib = (x[0],sorted(x[1],reverse = True))
    boro_top_three = []
    for i in range(3):
        boro_top_three+=((sorted_neib [0], sorted_neib [1][i]))
    return boro_top_three



if __name__ =='__main__':
    import sys
    sc = SparkContext()
    taxi = sc.textFile(sys.argv[1])

    Boros_top_three=taxi.mapPartitionsWithIndex(processTrips)\
            .reduceByKey(lambda x,y: x+y)\
            .map(lambda x: (x[0][0], (x[1], x[0][1]))).groupByKey()\
            .map(top_three).collect()
    print(Boros_top_three)


