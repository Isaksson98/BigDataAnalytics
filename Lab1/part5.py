from pyspark import SparkContext

sc = SparkContext(appName = "temperature lab part 5")

def max_value(a,b):
    if a>=b:
        return a
    else:
        return b

# This path is to the file on hdfs
stationsOstGot_csv = sc.textFile("BDA/input/stations-Ostergotland.csv")
precipitation_csv = sc.textFile("BDA/input/precipitation-readings.csv")

lines_stations_ostGot = stationsOstGot_csv.map(lambda line: line.split(";"))
lines_precip = precipitation_csv.map(lambda line: line.split(";"))


# (key, value) = ((station, year-month)),(rain,1))
rain_ostGot = lines_stations_ostGot.map(lambda x: ((x[0], x[1][0:7]), (float(x[3]), 1))) #Adding 1 for counting when averaging

# (key, value) =  ((station, year-month)),(rain,1))
rain_tot = lines_precip.map(lambda x: ((x[0], x[1][0:7]), (float(x[3]), 1)))

rain_tot = rain_tot.filter(lambda x: x[0][0] in rain_ostGot)

#Filter the years
rain_tot = rain_tot.filter(lambda x: int(x[0])>= 1993 and int(x[0])<=2016)

#Calculate average
month_avg = rain_tot.reduceByKey(lambda a,b: a+b).mapValues(lambda x: x[0]/x[1])

month_avg.coalesce(1, shuffle = False).saveAsTextFile("BDA/output/")