# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div  style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://raw.githubusercontent.com/derar-alhussein/Databricks-Certified-Data-Engineer-Professional/main/Includes/images/deletes.png" width="60%">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ### Here we want simply to provide a lookup table to be uesed to propogate deletes to the all tables that may contains a PII: customers and customers_orders

# COMMAND ----------

# MAGIC %run ../Includes/Copy-Datasets

# COMMAND ----------

from pyspark.sql import functions as F

schema = "customer_id STRING, email STRING, first_name STRING, last_name STRING, gender STRING, street STRING, city STRING, country_code STRING, row_status STRING, row_time timestamp"

(spark.readStream
        .table("bronze")
        .filter("topic = 'customers'")
        .select(F.from_json(F.col("value").cast("string"), schema).alias("v"))
        .select("v.*", F.col('v.row_time').alias("request_timestamp"))
        .filter("row_status = 'delete'")
        .select("customer_id", "request_timestamp",
                F.date_add("request_timestamp", 30).alias("deadline"), 
                F.lit("requested").alias("status"))
    .writeStream
        .outputMode("append")
        .option("checkpointLocation", "dbfs:/mnt/demo_pro/checkpoints/delete_requests")
        .trigger(availableNow=True)
        .table("delete_requests")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM delete_requests

# COMMAND ----------

# MAGIC %md
# MAGIC ### use the lookup table to perform the deletes on the existing data

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM customers_silver
# MAGIC WHERE customer_id IN (SELECT customer_id FROM delete_requests WHERE status = 'requested')

# COMMAND ----------

# MAGIC %md
# MAGIC ### test

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from customers_silver where customer_id = "C00405"

# COMMAND ----------

# MAGIC %md
# MAGIC ### As we have already activated CDF on customers_silver, we are going to use that to perform the deletes on only the changed data, remember, here the goal is to propogate deletes on streaming data
# MAGIC A second streaming process monitors changes in the customers_silver table using Delta Change Data Feed (CDF).
# MAGIC
# MAGIC Explanation:
# MAGIC
# MAGIC - Change Feed: Reads changes (_change_type) in the customers_silver table. Tracks deletions to propagate them to related tables.
# MAGIC - Starting Version: Ensures the stream processes changes from a specific Delta table version.
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

deleteDF = (spark.readStream
                 .format("delta")
                 .option("readChangeFeed", "true")
                 .option("startingVersion", 2)
                 .table("customers_silver"))

# COMMAND ----------

display(deleteDF)

# COMMAND ----------

# MAGIC %md
# MAGIC ### propogate the deletes to the customers_orders streaming table
# MAGIC
# MAGIC The process_deletes function handles deletions in two steps:
# MAGIC
# MAGIC - Delete From customers_orders: Deletes rows in the customers_orders table for the affected customer_ids.
# MAGIC
# MAGIC - Update delete_requests Status: Updates the delete_requests table, marking affected rows as "deleted", ensuring the deletion lifecycle is tracked.

# COMMAND ----------

def process_deletes(microBatchDF, batchId):
    
    # Delete From customers_orders: Deletes rows in the customers_orders table for the affected customer_ids.
    (microBatchDF
        .filter("_change_type = 'delete'")
        .createOrReplaceTempView("deletes")) # this is to create a temp view with "_change_type = 'delete'"

    microBatchDF._jdf.sparkSession().sql("""
        DELETE FROM customers_orders
        WHERE customer_id IN (SELECT customer_id FROM deletes)
    """)
    #  Update delete_requests Status: Updates the delete_requests table, marking affected rows as "deleted", ensuring the deletion lifecycle is tracked.
    
    microBatchDF._jdf.sparkSession().sql("""
        MERGE INTO delete_requests r
        USING deletes d
        ON d.customer_id = r.customer_id
        WHEN MATCHED
          THEN UPDATE SET status = "deleted"
    """)

# COMMAND ----------

# MAGIC %md
# MAGIC ### The deleteDF stream writes to a foreachBatch sink that invokes the process_deletes function for each micro-batch.
# MAGIC
# MAGIC
# MAGIC Explanation:
# MAGIC
# MAGIC - ForeachBatch: Executes the process_deletes logic for each micro-batch.
# MAGIC - Checkpointing: Ensures fault-tolerance and avoids reprocessing
# MAGIC

# COMMAND ----------

(deleteDF.writeStream
         .foreachBatch(process_deletes)
         .option("checkpointLocation", "dbfs:/mnt/demo_pro/checkpoints/deletes")
         .trigger(availableNow=True)
         .start())

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM delete_requests

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY customers_orders

# COMMAND ----------

# MAGIC %md
# MAGIC ### the deletes are not fully committed, they are still there in the previous versions and CDF versions, to fully commit the deletes, we have to perform vacuum command on these 2 tables

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM customers_orders@v6
# MAGIC EXCEPT
# MAGIC SELECT * FROM customers_orders

# COMMAND ----------

df = (spark.read
           .option("readChangeFeed", "true")
           .option("startingVersion", 2)
           .table("customers_silver")
           .filter("_change_type = 'delete'"))
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Flow Summary
# MAGIC - Capture Delete Requests: From bronze, store them in delete_requests with metadata (status, deadline).
# MAGIC - Propagate Deletes:
# MAGIC   - Remove records from customers_silver for requested deletes.
# MAGIC   - Monitor customers_silver changes and propagate deletions to customers_orders.
# MAGIC   - Update delete_requests status to "deleted".
# MAGIC - Lifecycle Tracking: Ensures every delete request is logged, processed, and tracked for audit and compliance.
