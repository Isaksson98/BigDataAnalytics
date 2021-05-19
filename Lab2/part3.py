from pyspark import SparkContext
from pyspark.sql import SQLContext, Row
from pyspark.sql import functions as F

sc = SparkContext(appName = "temperature lab part 3")
sqlContext = SQLContext(sc)

# This path is to the file on hdfs
temperatures_csv = sc.textFile("BDA/input/temperature-readings.csv")
lines = temperatures_csv.map(lambda line: line.split(";"))

tempReadingsRow = lines.map(lambda p: Row(id=p[0], year=p[1].split("-")[0], month=p[1].split("-")[1], day=p[1].split("-")[2], temperature=float(p[3])))
schemaTempReadings = sqlContext.createDataFrame(tempReadingsRow)
schemaTempReadings.registerTempTable("tempReadingsTable") 

#Filter the years
schemaTempReadings = schemaTempReadings.filter\
( (schemaTempReadings['year']>= 1950) & (schemaTempReadings['year']<= 2014))

#Calculate average
daily_max = schemaTempReadings.groupBy('day', 'month', 'year', 'id').agg(F.max("temperature").alias("temp"))
daily_min = schemaTempReadings.groupBy('day', 'month', 'year', 'id').agg(F.min("temperature").alias("temp"))
daily_avg = daily_max.union(daily_min).groupBy('day', 'month', 'year', 'id').avg("temp")


month_avg = daily_avg.groupBy('month', 'year', 'id').avg('avg(temp)').orderBy('avg(avg(temp))',ascending=False)

month_avg.rdd.coalesce(1).saveAsTextFile("BDA/output/")
