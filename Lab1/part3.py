from pyspark import SparkContext

sc = SparkContext(appName = "temperature lab part 3")

# This path is to the file on hdfs
temperatures_csv = sc.textFile("BDA/input/temperature-readings.csv")
lines = temperatures_csv.map(lambda line: line.split(";"))

# map  (key, value) = ((year-month-day, id),(temperature, 1))
year_Month_temperatures = lines.map(lambda x: ((x[1], x[0]), (float(x[3]))))
#Filter the years
year_Month_temperatures = year_Month_temperatures.filter(lambda x: int(x[0][0][0:4])>= 1950 and int(x[0][0][0:4])<=2014)

#Calculate average
daily_max = year_Month_temperatures.reduceByKey(lambda x,y: max(x,y))
daily_min = year_Month_temperatures.reduceByKey(lambda x,y: max(x,y))
daily_avg = daily_max.join(daily_min)\
.map(lambda x:  ((x[0][0][0:7], x[0][1]), ((x[1][0] + x[1][1])/2, 1)))
# map  (key, value) = ((year-month, id),(avg_daily_temperature, 1))

month_avg = daily_avg.reduceByKey(lambda a,b: a+b).mapValues(lambda x: x[0]/x[1])

month_avg.coalesce(1, shuffle = False).saveAsTextFile("BDA/output/")
