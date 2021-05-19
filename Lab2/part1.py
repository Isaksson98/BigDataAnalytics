from pyspark import SparkContext
from pyspark.sql import SQLContext, Row
from pyspark.sql import functions as F

sc = SparkContext(appName = "temperature lab part 1")
sqlContext = SQLContext(sc)

# This path is to the file on hdfs
temperatures_csv = sc.textFile("BDA/input/temperature-readings.csv")
lines = temperatures_csv.map(lambda line: line.split(";"))

tempReadingsRow = lines.map(lambda p: Row(id=p[0], year=p[1].split("-")[0], temperature=float(p[3])))
# Apply the schema to the RDD.
schemaTempReadings = sqlContext.createDataFrame(tempReadingsRow)
# Register the DataFrame as a table.
schemaTempReadings.registerTempTable("tempReadingsTable") 

#Filter the years
schemaTempReadings = schemaTempReadings.filter( (schemaTempReadings['year']>= 1950) & (schemaTempReadings['year']<= 2014))

#max_temp = year_temperature.reduceByKey(max_temperature)
max_temp = schemaTempReadings.groupBy('year').agg(F.max("temperature"))\
.orderBy('max(temperature)', ascending=False)

#min_temp = year_temperature.reduceByKey(min_temperature)
min_temp = schemaTempReadings.groupBy('year').agg(F.min("temperature"))\
.orderBy('min(temperature)',ascending=False)

max_temp.rdd.coalesce(1).saveAsTextFile("BDA/output/max")
min_temp.rdd.coalesce(1).saveAsTextFile("BDA/output/min")

# sbatch -A liu-compute-2021-4 --reservation liu-bda-2021-04-XX run_yarn_with_historyserver.q
# squeue -u x_filis

