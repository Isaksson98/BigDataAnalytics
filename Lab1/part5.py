from pyspark import SparkContext

sc = SparkContext(appName = "temperature lab part 4")

def max_value(a,b):
    if a>=b:
        return a
    else:
        return b


# This path is to the file on hdfs
stationsOstGot_csv = sc.textFile("BDA/input/stations-Ostergotland.csv")
precipitation_csv = sc.textFile("BDA/input/precipitation-readings.csv")

lines_stations = stationsOstGot_csv.map(lambda line: line.split(";"))
lines_precip = temperatures_csv.map(lambda line: line.split(";"))

#remove rain data not included in Ostergotland
lines_tot = lines_stations.join(lines_precip, lines_temp[0] == lines_precip[0],"leftouter")

# (key, value) = (year,rain)
temperature_rain = lines_tot.map(lambda x: (x[3][0:4], x[5])))

#Filter the years
year_temperature = year_temperature.filter(lambda x: int(x[0])>= 1993 and int(x[0])<=2016)

year_temperature = year_temperature.collect()
month_avg = year_temperature.groupByKey('months').avg()

month_avg.coalesce(1, shuffle = False).saveAsTextFile("BDA/output/part5/")