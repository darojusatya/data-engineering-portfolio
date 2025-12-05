from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("ETL").getOrCreate()

df = spark.read.csv("data/input.csv", header=True, inferSchema=True)
df2 = df.filter(df.quantity > 0)
df2.write.csv("data/output", header=True)

spark.stop()
