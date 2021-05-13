from pyspark import SparkContext

sc = SparkContext(appName = "temperature lab part 5")

# This path is to the file on hdfs
stationsOstGot_csv = sc.textFile("BDA/input/stations-Ostergotland.csv")
precipitation_csv = sc.textFile("BDA/input/precipitation-readings.csv")

lines_stations_ostGot = stationsOstGot_csv.map(lambda line: line.split(";"))
lines_precip = precipitation_csv.map(lambda line: line.split(";"))

#only maping to station id
# Collect() allows all the elements in the RDD to be returned
rain_ostGot = lines_stations_ostGot.map( lambda x: x[0] ).collect()
# (key, value) =  ((station, year-month)),(rain,1))
rain_tot = lines_precip.map(lambda x: ((x[0], x[1][0:7]), (float(x[3]))))

#Select only stations in OsterGotland
rain_tot = rain_tot.filter( lambda x: x[0][0] in rain_ostGot )

#print(13579)
#print( rain_ostGot[0:2] )
#print( rain_tot.collect()[0:3] )

#Filter the years
rain_tot = rain_tot.filter(lambda x: int(x[0][1][0:4])>= 1993 and int(x[0][1][0:4])<=2016)

#sum the total monthly precipitation for each station
sum_month = rain_tot.reduceByKey(lambda a,b: a+b)

print(13579)
print( sum_month.collect()[0] )

sum_month = sum_month.map(lambda x: (x[0][1], (float(x[1]), 1)))


#Calculate average over stations for a given month
month_avg = sum_month.reduceByKey(lambda a,b: a+b).mapValues(lambda x: x[0]/x[1])

#Sort
month_avg = month_avg.sortBy(ascending = False, keyfunc=lambda k: k[1])

month_avg.coalesce(1, shuffle = False).saveAsTextFile("BDA/output/")