#final_chall1.py
from pyspark.sql.functions import explode
from functools import partial
from pyspark import SparkContext
def wordList(_, rows):
    import re 
    separator = re.compile('\W+')
    n = 8   #max of 8 words
    for row in rows:
        count = []
        field = row.split('|')
        words = separator.split(field[5].lower())  #last second column
        wordlen = len(words)
        for i in range(wordlen):
            for j in range(1, n+1):
                if i+j > wordlen:
                    break        # stop when the max has reached
                count.append(' '.join(words[i:i+j])) #join all the words with space in between
        yield ((float(field[2]), float(field[1])), count)

# check for the correct match
def filterTweets(drug_words, _, rows):
    terms = drug_words.value
    for row in rows:
        lat_log, wordlist = row
        match = terms.intersection(wordlist)
        if len(match) > 0:
            yield(lat_log, match)

# Create index function 
def createIndex(shapefile):
    import rtree
    import fiona.crs
    import geopandas as gpd
    zones = gpd.read_file(shapefile).to_crs(fiona.crs.from_epsg(2263))
    index = rtree.Rtree()
    for idx,geometry in enumerate(zones.geometry):
        index.insert(idx, geometry.bounds)
    return (index, zones)

def findZone(p, index, zones):
    match = index.intersection((p.x, p.y, p.x, p.y))
    for idx in match:
        #check geometry valid or not
        if zones.geometry[idx].is_valid and zones.geometry[idx].contains(p):
            if zones.plctrpop10[idx]>0:
                return zones.plctract10[idx],zones.plctrpop10[idx]
    return None

def processTweets(pid, records):
    import pyproj
    import shapely.geometry as geom
    
    proj = pyproj.Proj(init="epsg:2263", preserve_units=True)
    index, zones = createIndex('500cities_tracts.geojson')
    counts = {}

    for row in records:
        lnglat, tweet = row
        p = geom.Point(proj(lnglat[0], lnglat[1]))
        zone = findZone(p, index, zones)
        if zone:
            if zone[1]>0:
                counts[zone] = counts.get(zone, 0) + 1
    return counts.items()

if __name__=='__main__':
    sc = SparkContext()    
    drugs1 = "drug_illegal.txt"
    drugs2 = "drug_sched2.txt"
    keywords = set(map(lambda x: x.strip(), 
                   (open(drugs1, 'r').readlines()+
                  open(drugs2, 'r').readlines())))    
    drug_words = sc.broadcast(keywords)
    output = sc.textFile('/data/share/bdm/tweets-100m.csv').mapPartitionsWithIndex(wordList) \
                    .mapPartitionsWithIndex(partial(filterTweets, drug_words))\
                    .mapPartitionsWithIndex(processTweets)\
                    .reduceByKey(lambda x,y: x+y)\
                     .map(lambda x:(x[0][0],float(x[1])/x[0][1]))\
                      .sortBy(lambda x: x[0]).saveAsTextFile('challenge1_output')
    

# spark-submit --conf spark.executorEnv.LD_LIBRARY_PATH=$LD_LIBRARY_PATH --executor-memory 24G --executor-cores 5 --num-executors 10 --driver-memory 5g --files hdfs:///data/share/bdm/500cities_tracts.geojson,hdfs:///data/share/bdm/drug_sched2.txt,hdfs:///data/share/bdm/drug_illegal.txt final_chall1.py hdfs:///data/share/bdm/tweets-100m.csv
#output  challenge1_output