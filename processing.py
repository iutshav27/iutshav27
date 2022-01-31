import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.functions import col
from pyspark.sql.functions import *
import pyspark.sql.functions as F


spark = SparkSession.builder.master('local').appName('Project_C').getOrCreate()

data = spark.read.csv('/home/futurense/Downloads/vehicles.csv',inferSchema=True,header=True)
print(data.printSchema())
#print(data.count())

data_filtered = data.select("id","region","price","year","manufacturer","model","condition","cylinders","fuel","odometer","title_status","transmission","VIN",
                            "drive","size","type","paint_color","posting_date")

#print(data_filtered.show(5,truncate=False))

data_filtered = data_filtered.na.drop()
print(data_filtered.show(10,truncate=False))

#data_filtered = data_filtered.withColumn('title_status_new', when(trim(data_filtered.title_status) == 'null','others'))
#print(data_filtered.groupby('title_status_new').count().show(truncate=False))
data_filtered = data_filtered.withColumn('Size_new', F.split(F.col('size'), "-").getItem(0))
data_filtered = data_filtered.withColumn('cylinders_new', F.split(F.col('cylinders'), " ").getItem(0))
data_filtered = data_filtered.drop('size','cylinders')
print(data_filtered.show(10, truncate=False))











