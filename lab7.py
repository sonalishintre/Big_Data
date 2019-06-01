def filterBike(pid, records):
    if pid==0:
        next(records)
    for record in records:
        fields = record.split(',')
        if (fields[6]=='Greenwich Ave & 8 Ave' and
            fields[3].startswith('2015-02-01')):
            yield (fields[3][:19], 1)

def filterTaxi(pid, lines):
    if pid==0:
        next(lines)
    import pyproj
    proj = pyproj.Proj(init="epsg:2263", preserve_units=True)
    station = proj(-74.00263761, 40.7390169)
    radius = 1320**2
    for trip in lines:
        fields = trip.split(',')
        if 'NULL' in fields[4:6]: continue
        dropoff = proj(fields[5], fields[4])
        distance = ((station[0]-dropoff[0])**2 +
                    (station[1]-dropoff[1])**2)
        if distance <= radius:
            yield(fields[1][:19], 0)

def connectTrips(_, records):
    import datetime
    lastTaxiTime = None
    count = 0
    for dt, mode in records:
        t = datetime.datetime.strptime(dt, '%Y-%m-%d %H:%M:%S')
        if mode==1:
            if lastTaxiTime!=None:
                diff = (t-lastTaxiTime).total_seconds()
                if diff>=0 and diff<=10:
                    count += 1
        else:
            lastTaxiTime = t
    yield count

from pyspark import SparkContext
if __name__ == "__main__":
    sc = SparkContext()

    taxi = sc.textFile('/data/share/bdm/yellow.csv.gz')
    bike = sc.textFile('/data/share/bdm/citibike.csv')
    matchedTaxi = taxi.mapPartitionsWithIndex(filterTaxi)
    matchedBike = bike.mapPartitionsWithIndex(filterBike)
    allTrips = (matchedBike+matchedTaxi).sortByKey()
    count = allTrips.mapPartitionsWithIndex(connectTrips).reduce(lambda x,y: x+y)
    sc.parallelize([count]).saveAsTextFile('tmp.txt')
