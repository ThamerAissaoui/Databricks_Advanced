# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div  style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://raw.githubusercontent.com/derar-alhussein/Databricks-Certified-Data-Engineer-Professional/main/Includes/images/orders.png" width="60%">
# MAGIC </div>

# COMMAND ----------

# MAGIC %run ../Includes/Copy-Datasets

# COMMAND ----------

(spark.read
      .table("bronze")
      .filter("topic = 'orders'")
      .count()
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### drop the duplicated orders with dropDuplicates()

# COMMAND ----------

from pyspark.sql import functions as F

json_schema = "order_id STRING, order_timestamp Timestamp, customer_id STRING, quantity BIGINT, total BIGINT, books ARRAY<STRUCT<book_id STRING, quantity BIGINT, subtotal BIGINT>>"

batch_total = (spark.read
                      .table("bronze")
                      .filter("topic = 'orders'")
                      .select(F.from_json(F.col("value").cast("string"), json_schema).alias("v"))
                      .select("v.*")
                      .dropDuplicates(["order_id", "order_timestamp"])
                      .count()
                )

print(batch_total)

# COMMAND ----------

# MAGIC %md
# MAGIC ### use the same logic with structred streaming, no duplicates between micro batches

# COMMAND ----------

# MAGIC %md
# MAGIC ### BUT this could increase treamandously the volume of states information to represent all history, we can limit the amount of states to be maintained by using Watermarking

# COMMAND ----------

deduped_df = (spark.readStream
                   .table("bronze")
                   .filter("topic = 'orders'")
                   .select(F.from_json(F.col("value").cast("string"), json_schema).alias("v"))
                   .select("v.*")
                   .withWatermark("order_timestamp", "30 seconds") # only track the state every 30s
                   .dropDuplicates(["order_id", "order_timestamp"]))

# COMMAND ----------

# MAGIC %md
# MAGIC ### make sure the inserted records are not already in the target table, use upsert logic with Merge

# COMMAND ----------

def upsert_data(microBatchDF, batch):
    microBatchDF.createOrReplaceTempView("orders_microbatch") # the new data to be inserted
    
    # define the query to upsert the data
    sql_query = """ 
      MERGE INTO orders_silver a
      USING orders_microbatch b
      ON a.order_id=b.order_id AND a.order_timestamp=b.order_timestamp
      WHEN NOT MATCHED THEN INSERT *
    """
    
    microBatchDF.sparkSession.sql(sql_query) # execute the query
    #microBatchDF._jdf.sparkSession().sql(sql_query)

# COMMAND ----------

# MAGIC %md
# MAGIC ### recreate the table, since we have dropped it in the previous notebook

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS orders_silver
# MAGIC (order_id STRING, order_timestamp Timestamp, customer_id STRING, quantity BIGINT, total BIGINT, books ARRAY<STRUCT<book_id STRING, quantity BIGINT, subtotal BIGINT>>)

# COMMAND ----------

query = (deduped_df.writeStream
                   .foreachBatch(upsert_data) # call upsert_data for each microbatch
                   .option("checkpointLocation", "dbfs:/mnt/demo_pro/checkpoints/orders_silver")
                   .trigger(availableNow=True)
                   .start())

query.awaitTermination()

# COMMAND ----------

streaming_total = spark.read.table("orders_silver").count()

print(f"batch total: {batch_total}")
print(f"streaming total: {streaming_total}")
