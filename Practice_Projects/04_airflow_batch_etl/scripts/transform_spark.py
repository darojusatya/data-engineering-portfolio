# simple spark transform reading data/raw.json and writing data/clean.parquet
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("airflow_transform").getOrCreate()
df = spark.read.json("data/raw.json")
df2 = df.filter(df.quantity>0).withColumnRenamed("date","sale_date")
df2.write.mode("overwrite").parquet("data/clean.parquet")
spark.stop()
print("transformed")
