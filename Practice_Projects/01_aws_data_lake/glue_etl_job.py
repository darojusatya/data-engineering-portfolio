# Simulated Glue PySpark ETL (runs locally with pyspark)
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, to_date

spark = SparkSession.builder.appName("glue_etl_sim").getOrCreate()

raw_path = "s3_sim/raw/sales.csv"
staged_path = "s3_sim/staged/sales_parquet"
curated_path = "s3_sim/curated/sales_partitioned"

df = spark.read.csv(raw_path, header=True, inferSchema=True)
# basic clean
df = df.filter(col("quantity").isNotNull() & (col("quantity")>0))
df = df.withColumn("total", col("quantity") * col("price"))
df = df.withColumn("sale_date", to_date(col("date"), "yyyy-MM-dd"))
# write staged
df.write.mode("overwrite").parquet(staged_path)
# write curated partitioned by sale_date
df.write.mode("overwrite").partitionBy("sale_date").parquet(curated_path)

print("ETL complete. Staged:", staged_path, "Curated:", curated_path)
spark.stop()
