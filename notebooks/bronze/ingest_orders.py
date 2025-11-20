from pyspark.sql import SparkSession

spark = SparkSession.getActiveSession()

df = spark.read.json("dbfs:/mnt/raw/orders")
df.write.format("delta").mode("overwrite").save("dbfs:/mnt/ecom/bronze/orders")
print("Ingestion of orders data completed successfully.")