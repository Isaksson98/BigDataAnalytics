from pyspark import SparkContext

sc = SparkContext(appName = "temperature lab part 1")

def max_temperature(a,b):
    if a>=b:
        return a
    else:
        return b

def min_temperature(a,b):
    if a>=b:
        return b
    else:
        return a

temperatures_csv = sc.texfile('<filepath>/temperature-readings.csv')
lines = temperatures_csv.map(lambda line: line.split(';'))
year_temperature = lines.map(lambda x: (x[1][0:4], float(x[3])))

#Filter the years
year_temperature = year_temperature.filter(lambda x: int(x[0])>= 1950 and int(x[0])<=2014)

max_temp = year_temperature.reduceByKey(max_temperature)
min_temp = year_temperature.reduceByKey(min_temperature)

max_temp_sorted = max_temp.sortBy(ascending = False, keyfunc=lambda k: k[1])
min_temp_sorted = max_temp.sortBy(ascending = False, keyfunc=lambda k: k[1])

max_temp_sorted.saveAsTextFile("<filepath>/output")
