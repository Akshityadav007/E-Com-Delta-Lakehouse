from pyspark.sql import SparkSession

spark = SparkSession.getActiveSession()

bronze = spark.read.format("delta").load("dbfs:/mnt/ecom/bronze/orders")
silver = bronze.filter("order_id IS NOT NULL")
silver.write.format("delta").mode("overwrite").save("dbfs:/mnt/ecom/silver/orders")
print("Silver orders table created successfully.")