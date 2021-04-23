from pyspark import SparkContext

sc = SparkContext(appName = "temperature lab part 2")

def temperature_over_10(temperature):
    if temperature>10:
        return 1
    else:
        return 0


temperatures_csv = sc.texfile('BDA/input_data/temperature-readings.csv')
lines = temperatures_csv.map(lambda line: line.split(';'))

year_Month_temperatures = lines.map(lambda x: (x[1][0:7], temperature_over_10(x[3]))) #0-7 include year & month

#Filter the years and if over 10
year_Month_temperatures = year_Month_temperatures.filter(lambda x: int(x[0])>= 1950 and int(x[0])<=2014)
year_Month_temperatures = year_Month_temperatures.filter(lambda x: int(x[3]) == 1)

#Count only unique values
year_Month_temperatures.distinct().count()

count_temp.saveAsTextFile("BDA/output/")
