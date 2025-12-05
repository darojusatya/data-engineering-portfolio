# PySpark Structured Streaming reading from Kafka and writing aggregated output
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, sum as _sum
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, LongType

spark = SparkSession.builder.appName("kafka_stream").getOrCreate()
kafka_bootstrap = "localhost:9092"
df = spark.readStream.format("kafka").option("kafka.bootstrap.servers", kafka_bootstrap).option("subscribe", "sales_events").load()
schema = StructType([
    StructField("order_id", IntegerType()),
    StructField("customer_id", IntegerType()),
    StructField("quantity", IntegerType()),
    StructField("price", DoubleType()),
    StructField("timestamp", LongType())
])
json_df = df.select(from_json(col("value").cast("string"), schema).alias("data"))
parsed = json_df.select("data.*")
agg = parsed.withColumn("revenue", col("quantity") * col("price")).groupBy(window(col("timestamp").cast("timestamp"), "1 minute")).agg(_sum("revenue").alias("minute_revenue"))
query = agg.writeStream.format("parquet").option("path","stream_output").option("checkpointLocation","stream_checkpoint").outputMode("complete").start()
query.awaitTermination()
