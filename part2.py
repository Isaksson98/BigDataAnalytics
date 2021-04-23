from pyspark import SparkContext

sc = SparkContext(appName = "temperature lab part 2")

def temperature_over_10(temperature):
    if temperature>=10:
        return 1
    else:
        return 0


temperatures_csv = sc.texfile('<filepath>/temperature-readings.csv')
lines = temperatures_csv.map(lambda line: line.split(';'))

year_temperature = lines.map(lambda x: (x[1][0:4], float(x[3])))
