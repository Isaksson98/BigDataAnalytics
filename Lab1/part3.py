from pyspark import SparkContext

sc = SparkContext(appName = "temperature lab part 3")

# This path is to the file on hdfs
temperatures_csv = sc.textFile("BDA/input/temperature-readings.csv")
lines = temperatures_csv.map(lambda line: line.split(";"))

# map  (key, value) = ((year-month, id),(temperature, 1))
year_Month_temperatures = lines.map(lambda x: ((x[1][0:7], x[0]), (float(x[3]), 1))) #Adding 1 for counting when averaging
#Filter the years
year_Month_temperatures = year_Month_temperatures.filter(lambda x: int(x[0][0][0:4])>= 1950 and int(x[0][0][0:4])<=2014)

#Calculate average
month_avg = year_Month_temperatures.reduceByKey(lambda a,b: a+b).mapValues(lambda x: x[0]/x[1])

month_avg.coalesce(1, shuffle = False).saveAsTextFile("BDA/output/")
