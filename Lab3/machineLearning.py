from __future__ import division
from math import radians, cos, sin, asin, sqrt, exp
from datetime import datetime
from pyspark import SparkContext
sc = SparkContext(appName="lab_kernel")

def gaussianKernel(x_diff, h):
    u = x_diff/h
    kernel = exp(-(u)**2)
    return kernel

def dist_date (date1, date2):
    d1 = datetime(int(date1[0:4]), int(date1[5:7]), int(date1[8:10]))
    d2 = datetime(int(date2[0:4]), int(date2[5:7]), int(date2[8:10]))

    x = (int(date1[0:4])-int(date2[0:4])) % 365
    
    if (x > 182):
        x = 365 - x
    
    return x

def dist_time (time1, time2):

    x = abs( int(time2)-int(time1) )
    #x = as.numeric(difftime(time1, time2, units = "hours") )

    if (x > 12):
      x = 24 - x
  
    return x


def haversine(lon1, lat1, lon2, lat2):
    """
    Calculate the great circle distance between two points
    on the earth (specified in decimal degrees)
    """
    # convert decimal degrees to radians
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
    # haversine formula
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a))
    km = 6367 * c
    return km

h_distance = 200000 # Up to you
h_date = 15         # Up to you
h_time = 4          # Up to you

a = 58.4274 # Up to you
b = 14.826 # Up to you

date = "2013-07-04" # Up to you

stations = sc.textFile("BDA/input/stations.csv")
temps = sc.textFile("BDA/input/temperature-readings.csv")

# Your code here
lines_temps = temps.map(lambda line: line.split(";"))
lines_stations = stations.map(lambda line: line.split(";"))

# (key, value) = (id,(date, time, temperature))
temperature = lines_temps.map(lambda x: (x[0], (x[1], x[2], float(x[3]))))
# (key, value) = (id,(latitude, longitude))
stations = lines_stations.map(lambda x: (x[0], (float(x[3]), float(x[4]))))

#print(123)
#print(temperature.take(1))

temperature = temperature.sample(False, 0.1) #Only use 10% of data when testing

# Merging stations and temperature rdd
temperature = temperature.join(stations)
#print(456)
#print(temperature.take(1))

#Filter the date
temperature = temperature.filter(lambda x:  int(x[0][0:4]) <= int(date[0:4]))

#Cache the data
#temperature.cache()
#print(789)
#print(temperature.take(1))

prediction_sum = [] #store predictions, sum of kernels
prediction_prod = [] #store predictions, product of kernels


for time in [24, 22, 20, 18, 16, 14,
12, 10, 8, 6, 4]:

    #(id, ( (date, time, temp), (lat, long) ) ) 
    kernels = temperature.map( lambda x: \
    (   gaussianKernel( dist_time( time, x[1][0][1][0:2] ) , h_time), \
        gaussianKernel( haversine(a, b, x[1][1][0], x[1][1][1]), h_distance ), \
        gaussianKernel( dist_date(date, x[1][0][0]) , h_date),
        x[1][0][2]        
    ))
    #print(time)
    #print(kernels.take(1))
    #summing the kernels multiplied by temperature
    kernels_sum = kernels.map( lambda x: (x[0]*x[3]+x[1]*x[3]+x[2]*x[3], x[0]+x[1]+x[2]) ) #x[3] is temperature
    kernels_prod = kernels.map( lambda x: (x[0]*x[1]*x[2]*x[3], x[0]*x[1]*x[2]) ) #numerator, denominator
    #print('output')
    #print(time)
    #print(kernels_sum.take(5))

    #Sum
    kernels_prod = kernels_prod.reduce(lambda x, y: (x[0]+y[0], x[1]+y[1]))
    kernels_sum = kernels_sum.reduce(lambda x, y: (x[0]+y[0], x[1]+y[1]))

    #Prediction
    prediction_sum.append( (time, kernels_sum[0]/kernels_sum[1]) )
    prediction_prod.append( (time, kernels_prod[0]/kernels_prod[1]) )

#prediction_sum.coalesce(1).saveAsTextFile("BDA/output/sum")
print('----------------output-------------------')
print(prediction_sum)
print(prediction_prod)



