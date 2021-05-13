from pyspark import SparkContext
from pyspark.sql import SQLContext, Row
from pyspark.sql import functions as F

sc = SparkContext(appName = "temperature lab part 3")
sqlContext = SQLContext(sc)

# This path is to the file on hdfs
temperatures_csv = sc.textFile("BDA/input/temperature-readings.csv")
lines = temperatures_csv.map(lambda line: line.split(";"))

tempReadingsRow = lines.map(lambda p: Row(id=p[0], year=p[1].split("-")[0], month=p[1].split("-")[1], temperature=float(p[3])))
schemaTempReadings = sqlContext.createDataFrame(tempReadingsRow)
schemaTempReadings.registerTempTable("tempReadingsTable") 

#Filter the years
schemaTempReadings = schemaTempReadings.filter\
( (schemaTempReadings['year']>= 1950) & (schemaTempReadings['year']<= 2014))

#Calculate average
month_avg = schemaTempReadings.groupBy('month', 'year', 'id').avg('temperature')

month_avg.rdd.coalesce(1).saveAsTextFile("BDA/output/")
