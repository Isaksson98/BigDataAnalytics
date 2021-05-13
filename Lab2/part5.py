from pyspark import SparkContext
from pyspark.sql import SQLContext, Row
from pyspark.sql import functions as F

sc = SparkContext(appName = "temperature lab part 5")
sqlContext = SQLContext(sc)

# This path is to the file on hdfs
stationsOstGot_csv = sc.textFile("BDA/input/stations-Ostergotland.csv")
precipitation_csv = sc.textFile("BDA/input/precipitation-readings.csv")

lines_stations_ostGot = stationsOstGot_csv.map(lambda line: line.split(";"))
lines_precip = precipitation_csv.map(lambda line: line.split(";"))

#only maping to station id
stations_ostGot = lines_stations_ostGot.map(lambda p: Row(id=p[0]))
schemaOstGot = sqlContext.createDataFrame(stations_ostGot)
schemaOstGot.registerTempTable("OstGotTable") 

precipReadingsRow = lines_precip.map(lambda p: Row(id=p[0], year=p[1].split("-")[0], month=p[1].split("-")[1], precipitation=float(p[3])))
schemaPrecipReadings = sqlContext.createDataFrame(precipReadingsRow)
schemaPrecipReadings.registerTempTable("precipReadingsTable") 

#Select only stations in OsterGotland
SchemaOstGotRain = schemaPrecipReadings.join(schemaOstGot, 'id', 'inner')

#Filter the years
SchemaOstGotRain = SchemaOstGotRain.filter\
( (SchemaOstGotRain['year']>= 1993) & (SchemaOstGotRain['year']<= 2016) )

#sum the total monthly precipitation for each station
SchemaOstGotRain = SchemaOstGotRain.groupBy('year', 'month', 'id').sum('precipitation')
SchemaOstGotRain = SchemaOstGotRain.groupBy('year', 'month').agg(F.avg('sum(precipitation)'))\
.orderBy('avg(sum(precipitation))', ascending=False)


SchemaOstGotRain.rdd.coalesce(1).saveAsTextFile("BDA/output/")