from pyspark import SparkContext

sc = SparkContext(appName = "temperature lab part 2")

def temperature_over_10(temperature):
    if temperature>10:
        return 1
    else:
        return 0

# This path is to the file on hdfs
temperatures_csv = sc.textFile("BDA/input/temperature-readings.csv")
lines = temperatures_csv.map(lambda line: line.split(";"))

# map  (key, value) = (year-month, (id, temperature_over_10))
year_Month_temperatures = lines.map(lambda x: (x[1][0:7], (x[0], temperature_over_10(float(x[3]))))) #0-7 include year & month

#Filter the years 1950-2014
year_Month_temperatures = year_Month_temperatures.filter(lambda x: int(x[0][0:4])>= 1950 and int(x[0][0:4])<=2014)


#for part 2 of this task add this line: only count reading from station in a month once
year_Month_temperatures = year_Month_temperatures.distinct()
#Remove station id:  (key, value) = (year-month, temperature_over_10)
year_Month_temperatures = year_Month_temperatures.map(lambda x: (x[0], x[1][1]))

year_Month_temperatures = year_Month_temperatures.reduceByKey(lambda v1,v2: v1+v2)

#Sort by year-month
year_Month_temperatures = year_Month_temperatures.sortBy(ascending = False, keyfunc=lambda k: k[0])

year_Month_temperatures.coalesce(1, shuffle = False).saveAsTextFile("BDA/output/")
