from pyspark import SparkContext
from pyspark.sql import SQLContext, Row
from pyspark.sql import functions as F

sc = SparkContext(appName = "temperature lab part 4")
sqlContext = SQLContext(sc)

# This path is to the file on hdfs
temperatures_csv = sc.textFile("BDA/input/temperature-readings.csv")
precipitation_csv = sc.textFile("BDA/input/precipitation-readings.csv")

lines_temperature = temperatures_csv.map(lambda line: line.split(";"))
lines_precip = precipitation_csv.map(lambda line: line.split(";"))

tempReadingsRow = lines_temperature.map(lambda p: Row(id=p[0], temperature=float(p[3])))
schemaTempReadings = sqlContext.createDataFrame(tempReadingsRow)
schemaTempReadings.registerTempTable("tempReadingsTable") 


precipReadingsRow = lines_precip.map(lambda p: Row(id=p[0], precipitation=float(p[3])))
schemaPrecipReadings = sqlContext.createDataFrame(precipReadingsRow)
schemaPrecipReadings.registerTempTable("precipReadingsTable") 

#Filter temperature
schemaTempReadings = schemaTempReadings.filter\
( (schemaTempReadings['temperature']>= 25) & (schemaTempReadings['temperature']<= 30))

#Filter rain
schemaPrecipReadings = schemaPrecipReadings.filter\
( (schemaPrecipReadings['precipitation']>= 100) & (schemaPrecipReadings['precipitation']<= 200))


#Find maximum temp
max_temp = schemaTempReadings.groupBy('id').agg(F.max("temperature"))\
.orderBy(['id','max(temperature)'],ascending=[1,0])

#Find maximum rain
max_rain = schemaPrecipReadings.groupBy('id').agg(F.max("precipitation"))\
.orderBy(['id','max(precipitation)'],ascending=[1,0])


#merge temperature & precipitation data
output = max_temp.join(max_rain, 'id', 'inner')

output.rdd.coalesce(1).saveAsTextFile("BDA/output/")
