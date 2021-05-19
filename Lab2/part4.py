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

#Find maximum temp
max_temp = schemaTempReadings.groupBy('id').agg(F.max("temperature")).alias('temperature')\
.orderBy(['id','max(temperature)'],ascending=[1,0])

#Find maximum rain
max_rain = schemaPrecipReadings.groupBy('id').agg(F.max("precipitation")).alias('precipitation')\
.orderBy(['id','max(precipitation)'],ascending=[1,0])


#Filter temperature
max_temp = max_temp.filter\
( (max_temp['max(temperature)']>= 25) & (max_temp['max(temperature)']<= 30))

#Filter rain
max_rain = max_rain.filter\
( (max_rain['max(precipitation)']>= 100) & (max_rain['max(precipitation)']<= 200))



#merge temperature & precipitation data
output = max_temp.join(max_rain, 'id', 'inner').orderBy('id', ascending=False)

output.rdd.coalesce(1).saveAsTextFile("BDA/output/")
