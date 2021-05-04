from pyspark import SparkContext

sc = SparkContext(appName = "temperature lab part 4")

def max_value(a,b):
    if a>=b:
        return a
    else:
        return b

# This path is to the file on hdfs
temperatures_csv = sc.textFile("BDA/input/temperature-readings.csv")
precipitation_csv = sc.textFile("BDA/input/precipitation-readings.csv")

lines_temperature = temperatures_csv.map(lambda line: line.split(";"))
lines_precip = precipitation_csv.map(lambda line: line.split(";"))

# (key, value) = (station,temperature)
temperature = lines_temperature.map(lambda x: (x[0], float(x[3])))
# (key, value) = (station,rain)
rain = lines_precip.map(lambda x: (x[0], float(x[3])))

#Filter temperature
temperature = temperature.filter(lambda x: x[1]>= 25 and x[1]<= 30)
#Filter rain
rain = rain.filter(lambda x: x[1]>= 100 and x[1]<= 200)

#Find maximum temp
max_temp = temperature.reduceByKey(max_value)
#Find maximum rain
max_rain = rain.reduceByKey(max_value) #lambda a,b : max(a,b))

#merge temperature & precipitation data
output = max_temp.join(max_rain)

output.coalesce(1, shuffle = False).saveAsTextFile("BDA/output/")
