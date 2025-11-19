from pyspark.sql import SparkSession

spark = SparkSession.getActiveSession()

silver = spark.read.format("delta").load("dbfs:/mnt/ecom/silver/orders")
agg = silver.groupBy("customer_id").count()
agg.write.format("delta").mode("overwrite").save("dbfs:/mnt/ecom/gold/orders_by_customer")
