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

# map  (key, value) = (year-month,temperature_over_10)
year_Month_temperatures = lines.map(lambda x: (x[1][0:7], temperature_over_10(x[3]))) #0-7 include year & month

#Filter the years 1950-2014
year_Month_temperatures = year_Month_temperatures.filter(lambda x: int(x[0][0:4])>= 1950 and int(x[0][0:4])<=2014)

#NOT SURE ABOUT THIS LINE: Filter so if station above 10 in some month, appears only once
year_Month_temperatures = year_Month_temperatures.filter(lambda x: int(x[3]) == 1)

#NOT SURE HERE: Count only unique values
year_Month_temperatures.distinct().count()

year_Month_temperatures.coalesce(1, shuffle = False).saveAsTextFile("BDA/output/part2/over10")
