from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, row_number
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("retail_analytics").getOrCreate()
df = spark.read.csv("data/orders_large.csv", header=True, inferSchema=True)
# revenue per customer
rev = df.withColumn("revenue", col("quantity")*col("price")).groupBy("customer_id").agg(_sum("revenue").alias("total_revenue"))
# rank top customers
window = Window.orderBy(col("total_revenue").desc())
ranked = rev.withColumn("rank", row_number().over(window))
ranked.filter(col("rank")<=10).show()
# write optimized parquet partitioned by customer_id%10
ranked.write.mode("overwrite").parquet("data/retail_analytics_parquet")
print("analytics complete")
spark.stop()
