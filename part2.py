from pyspark import SparkContext

sc = SparkContext(appName = "temperature lab part 2")

def temperature_over_10(temperature):
    if temperature>10:
        return 1
    else:
        return 0


temperatures_csv = sc.texfile('<filepath>/temperature-readings.csv')
lines = temperatures_csv.map(lambda line: line.split(';'))

year_Month_temperatures = lines.map(lambda x: (x[1][0:7], float(x[3]))) #0-7 include year & month

#Filter the years
year_Month_temperatures = year_Month_temperatures.filter(lambda x: int(x[0])>= 1950 and int(x[0])<=2014)

#Find all that are over 10 degrees
over_10 = year_Month_temperatures.reduceByKey(temperature_over_10)

#Count how many meassurements for every month
count_temp = over_10.reduceByKey(lambda v1, v2: v1+v2)

count_temp.saveAsTextFile("<filepath>/output")
