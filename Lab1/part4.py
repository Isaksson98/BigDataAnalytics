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
lines_precip = temperatures_csv.map(lambda line: line.split(";"))

#merge temperature & precipitation data
lines_tot = lines_temperature.join(lines_precip, lines_temp[0] == lines_precip[0])

# (key, value) = (station,(temperature,rain))
temperature_rain = lines_tot.map(lambda x: (x[0], (x[3],x[5])))

#Filter temperature
temperature_rain = temperature_rain.filter(lambda x: int(x[3])>= 25 and int(x[3])<= 30)
#Filter rain
temperature_rain = temperature_rain.filter(lambda x: int(x[5])>= 100 and int(x[5])<= 200)

#Find maximum temp and rain
max_temp = lines_tot.reduceByKey(max_value)

max_temp.coalesce(1, shuffle = False).saveAsTextFile("BDA/output/part4/")

