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
# This path is to the file on hdfs
temperatures_csv = sc.textFile("BDA/input/temperature-readings.csv")
lines = temperatures_csv.map(lambda line: line.split(";"))

# (key, value) = (year,(id,temperature))
year_temperature = lines.map(lambda x: (x[1][0:4], (x[0],float(x[3]))))

#Filter the years
year_temperature = year_temperature.filter(lambda x: int(x[0])>= 1950 and int(x[0])<=2014)

max_temp = year_temperature.reduceByKey(max_temperature)
min_temp = year_temperature.reduceByKey(min_temperature)

max_temp_sorted = max_temp.sortBy(ascending = False, keyfunc=lambda k: k[1])
min_temp_sorted = min_temp.sortBy(ascending = False, keyfunc=lambda k: k[1])

# add join
max_temp_sorted.coalesce(1, shuffle = False).saveAsTextFile("BDA/output/max")
min_temp_sorted.coalesce(1, shuffle = False).saveAsTextFile("BDA/output/min")

# sbatch -A liu-compute-2021-4 --reservation liu-bda-2021-04-XX run_yarn_with_historyserver.q
# squeue -u x_filis