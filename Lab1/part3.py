from pyspark import SparkContext

sc = SparkContext(appName = "temperature lab part 3")

# This path is to the file on hdfs
temperatures_csv = sc.textFile("BDA/input/temperature-readings.csv")
lines = temperatures_csv.map(lambda line: line.split(";"))

# map  (key, value) = (year-month,(id,temperature))
year_Month_temperatures = lines.map(lambda x: (x[1][0:7], (x[0],float(x[3]))) #0-7 include year & month
#Filter the years
year_Month_temperatures = year_Month_temperatures.filter(lambda x: int(x[0][0:4])>= 1950 and int(x[0])[0:4]<=2014)

year_Month_temperatures.groupByKey()
#month_average = year_Month_temperatures.map(lambda x: (x[0], x[4], avg(x[3]))) #year, station, average temperature

month_avg = lines.groupByKey('months').avg()

count_temp.coalesce(1, shuffle = False).saveAsTextFile("BDA/output/part3/")
