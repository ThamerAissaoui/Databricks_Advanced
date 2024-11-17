# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div  style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://raw.githubusercontent.com/derar-alhussein/Databricks-Certified-Data-Engineer-Professional/main/Includes/images/orders.png" width="60%">
# MAGIC </div>

# COMMAND ----------

# MAGIC %run ../Includes/Copy-Datasets

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT cast(key AS STRING), cast(value AS STRING) -- key BINARY, value BINARY so we should convert them to a readable format
# MAGIC FROM bronze
# MAGIC LIMIT 20

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT v.* -- parsing the orders data using from_json function
# MAGIC FROM (
# MAGIC   SELECT from_json(cast(value AS STRING), "order_id STRING, order_timestamp Timestamp, customer_id STRING, quantity BIGINT, total BIGINT, books ARRAY<STRUCT<book_id STRING, quantity BIGINT, subtotal BIGINT>>") v
# MAGIC   FROM bronze
# MAGIC   WHERE topic = "orders")

# COMMAND ----------

(spark.readStream # convert our static table into a streaming table view 
      .table("bronze")
      .createOrReplaceTempView("bronze_tmp"))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT v.*  -- use that same streaming table view to to parse the orders data using from_json function
# MAGIC FROM (
# MAGIC   SELECT from_json(cast(value AS STRING), "order_id STRING, order_timestamp Timestamp, customer_id STRING, quantity BIGINT, total BIGINT, books ARRAY<STRUCT<book_id STRING, quantity BIGINT, subtotal BIGINT>>") v
# MAGIC   FROM bronze_tmp
# MAGIC   WHERE topic = "orders")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW orders_silver_tmp AS -- now save the previous result to a view (bridge view)
# MAGIC   SELECT v.*
# MAGIC   FROM (
# MAGIC     SELECT from_json(cast(value AS STRING), "order_id STRING, order_timestamp Timestamp, customer_id STRING, quantity BIGINT, total BIGINT, books ARRAY<STRUCT<book_id STRING, quantity BIGINT, subtotal BIGINT>>") v
# MAGIC     FROM bronze_tmp
# MAGIC     WHERE topic = "orders")

# COMMAND ----------

query = (spark.table("orders_silver_tmp")  # use that view in Python to create the silver orders table
               .writeStream
               .option("checkpointLocation", "dbfs:/mnt/demo_pro/checkpoints/orders_silver")
               .trigger(availableNow=True) # all records will be processed in multiple micro-batches until no more data is avaialable and then stops
               .table("orders_silver"))

query.awaitTermination()

# COMMAND ----------

# MAGIC %md
# MAGIC ###BUT instead of doing all the previous steps and the temp view we could have simply use the spark sql logic with filters

# COMMAND ----------

from pyspark.sql import functions as F 

json_schema = "order_id STRING, order_timestamp Timestamp, customer_id STRING, quantity BIGINT, total BIGINT, books ARRAY<STRUCT<book_id STRING, quantity BIGINT, subtotal BIGINT>>"

query = (spark.readStream.table("bronze")
        .filter("topic = 'orders'") # we only need orders
        .select(F.from_json(F.col("value").cast("string"), json_schema).alias("v")) #casting the value from binary to string and parsing it as json, give it an alias as v
        .select("v.*")
     .writeStream
        .option("checkpointLocation", "dbfs:/mnt/demo_pro/checkpoints/orders_silver")
        .trigger(availableNow=True)
        .table("orders_silver"))

query.awaitTermination()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM orders_silver
