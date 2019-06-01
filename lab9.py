def processTrips(pid, records):
    import csv
    import pyproj
    import shapely.geometry as geom
    import geopandas as gpd
    import fiona
    import fiona.crs
    import rtree

   
    index = rtree.Rtree()
    neighbors = gpd.read_file('neighborhoods.geojson').to_crs(fiona.crs.from_epsg(2263))
    for idx,geometry in enumerate(neighbors.geometry):
        index.insert(idx, geometry.bounds)

    proj = pyproj.Proj(init="epsg:2263", preserve_units=True)

    if pid==0:
        next(records)

    reader = csv.reader(records)
    for row in reader:
        try:
            p = geom.Point(proj(float(row[9]), float(row[10])))
            for idx in index.intersection((p.x, p.y, p.x, p.y)):
                if neighbors.geometry[idx].contains(p):
                    borough = neighbors.borough[idx]
            if borough:
                b = geom.Point(proj(float(row[5]), float(row[6])))
                for idx in index.intersection((p.x, p.y, p.x, p.y)):
                    if neighbors.geometry[idx].contains(p):
                         neighborhood = neighbors.neighborhood[idx]
            if neighborhood and borough:
                yield((neighborhood,borough),1)
        except:
            pass
if __name__ == '__main__':
    from pyspark import SparkContext
    sc = SparkContext()
    rdd = sc.textFile('/data/share/bdm/yellow_tripdata_2011-05.csv')
    output =rdd.mapPartitionsWithIndex(processTrips)\
             .reduceByKey(lambda x,y: x+y)\
             .map(lambda x: (x[0][1], (x[1],x[0][0]))).groupByKey()\
             .map(lambda x: (x[0],sorted(x[1],reverse= True)))\
             .map(lambda x: (x[0],x[1][:3])).saveAsTextFile('finaloutput')
    
    
#commands used to run on cluster
#vim lab9.py
# spark-submit --num-executors 5 --executor-cores 5 --files hdfs:///data/share/bdm/neighborhoods.geojson lab9.py



    