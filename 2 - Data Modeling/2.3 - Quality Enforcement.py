# Databricks notebook source
# MAGIC %run ../Includes/Copy-Datasets

# COMMAND ----------

# MAGIC %md
# MAGIC ### Adding CONSTRAINTS for Quality Enforcement

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE orders_silver ADD CONSTRAINT timestamp_within_range CHECK (order_timestamp >= '2020-01-01');

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED orders_silver

# COMMAND ----------

# MAGIC %md
# MAGIC ### Try to violate table contraint with inserting record number 2

# COMMAND ----------

# MAGIC %md
# MAGIC ### in datawarehouses all transacations respects  ACID : Atomicity, Consistency, Isolation and Durability, here for Atomicity: The transaction executes completely or not at all, which means that the following query will fail

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO orders_silver
# MAGIC VALUES ('1', '2022-02-01 00:00:00.000', 'C00001', 0, 0, NULL),
# MAGIC        ('2', '2019-05-01 00:00:00.000', 'C00001', 0, 0, NULL),
# MAGIC        ('3', '2023-01-01 00:00:00.000', 'C00001', 0, 0, NULL)

# COMMAND ----------

# MAGIC %md
# MAGIC ### which means none of this records had been inserted

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM orders_silver
# MAGIC WHERE order_id IN ('1', '2', '3')

# COMMAND ----------

# MAGIC %md
# MAGIC ### 24 rows in hive_metastore.bookstore_eng_pro.orders_silver violate the new CHECK constraint: there are violations of this constraint already, so this constraint CANNOT be added to the table

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE orders_silver ADD CONSTRAINT valid_quantity CHECK (quantity > 0);

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED orders_silver

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM orders_silver
# MAGIC where quantity <= 0

# COMMAND ----------

# MAGIC %md
# MAGIC ### Solution: we can simply add the contraint as a filter from the get-go while creating the table with spark sql

# COMMAND ----------

from pyspark.sql import functions as F

json_schema = "order_id STRING, order_timestamp Timestamp, customer_id STRING, quantity BIGINT, total BIGINT, books ARRAY<STRUCT<book_id STRING, quantity BIGINT, subtotal BIGINT>>"

query = (spark.readStream.table("bronze")
        .filter("topic = 'orders'")
        .select(F.from_json(F.col("value").cast("string"), json_schema).alias("v"))
        .select("v.*")
        .filter("quantity > 0")
     .writeStream
        .option("checkpointLocation", "dbfs:/mnt/demo_pro/checkpoints/orders_silver")
        .trigger(availableNow=True)
        .table("orders_silver"))

query.awaitTermination()

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE orders_silver DROP CONSTRAINT timestamp_within_range;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED orders_silver

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE orders_silver

# COMMAND ----------

# MAGIC %md
# MAGIC ### delete the checkpoint of the table

# COMMAND ----------

dbutils.fs.rm("dbfs:/mnt/demo_pro/checkpoints/orders_silver", True)
