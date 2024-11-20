# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div  style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://raw.githubusercontent.com/derar-alhussein/Databricks-Certified-Data-Engineer-Professional/main/Includes/images/customers.png" width="60%">
# MAGIC </div>

# COMMAND ----------

# MAGIC %run ../Includes/Copy-Datasets

# COMMAND ----------

# MAGIC %md
# MAGIC ### Here we will be creating the customer silver table

# COMMAND ----------

# MAGIC %sql
# MAGIC select cast(key as string), cast (value as string)
# MAGIC from bronze
# MAGIC

# COMMAND ----------

from pyspark.sql import functions as F

schema = "customer_id STRING, email STRING, first_name STRING, last_name STRING, gender STRING, street STRING, city STRING, country_code STRING, row_status STRING, row_time timestamp"

customers_df = (spark.table("bronze")
                 .filter("topic = 'customers'") # get only the customer by topic
                 .select(F.from_json(F.col("value").cast("string"), schema).alias("v")) # convert key and value from binary to strting and give itan alias
                 .select("v.*")
                 .filter(F.col("row_status").isin(["insert", "update"]))
                 .orderBy("customer_id")) #filtering on only insert and update

display(customers_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### keep only the most recent record by assigning a rank to each customer_id beased on a window

# COMMAND ----------

from pyspark.sql.window import Window

window = Window.partitionBy("customer_id").orderBy(F.col("row_time").desc())

ranked_df = (customers_df.withColumn("rank", F.rank().over(window)) # keep only the most recent record by assigning a rank to each customer_id beased on a window
                          .filter("rank == 1") # based on that time window, get the first rank of each record
                          .drop("rank")) # drop the rank column as it is not needed anymore
display(ranked_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Try to apply the same logic to a streaming read

# COMMAND ----------

# This will throw an exception because non-time-based window operations are not supported on streaming DataFrames.
ranked_df = (spark.readStream
                   .table("bronze")
                   .filter("topic = 'customers'")
                   .select(F.from_json(F.col("value").cast("string"), schema).alias("v"))
                   .select("v.*")
                   .filter(F.col("row_status").isin(["insert", "update"]))
                   .withColumn("rank", F.rank().over(window))
                   .filter("rank == 1")
                   .drop("rank")
             )

display(ranked_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Solution: using microbatch instead of streaming

# COMMAND ----------

from pyspark.sql.window import Window

def batch_upsert(microBatchDF, batchId):
    window = Window.partitionBy("customer_id").orderBy(F.col("row_time").desc())
    
    (microBatchDF.filter(F.col("row_status").isin(["insert", "update"]))
                 .withColumn("rank", F.rank().over(window))
                 .filter("rank == 1")
                 .drop("rank")
                 .createOrReplaceTempView("ranked_updates"))
    # create the ranked_updates as we did before, but this time it will be used in micro batch
   
    # upsert data between customers_silver and ranked_updates
    query = """
        MERGE INTO customers_silver c
        USING ranked_updates r
        ON c.customer_id=r.customer_id
            WHEN MATCHED AND c.row_time < r.row_time
              THEN UPDATE SET *
            WHEN NOT MATCHED
              THEN INSERT *
    """
    
    microBatchDF.sparkSession.sql(query)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS customers_silver
# MAGIC (customer_id STRING, email STRING, first_name STRING, last_name STRING, gender STRING, street STRING, city STRING, country STRING, row_time TIMESTAMP)

# COMMAND ----------

df_country_lookup = spark.read.json(f"{dataset_bookstore}/country_lookup")
display(df_country_lookup)

# COMMAND ----------

query = (spark.readStream
                  .table("bronze")
                  .filter("topic = 'customers'")
                  .select(F.from_json(F.col("value").cast("string"), schema).alias("v"))
                  .select("v.*")
                  .join(F.broadcast(df_country_lookup), F.col("country_code") == F.col("code") , "inner") # broadcast function for a small data sets df_country_lookup that could fit in memory for all executors
               .writeStream
                  .foreachBatch(batch_upsert)
                  .option("checkpointLocation", "dbfs:/mnt/demo_pro/checkpoints/customers_silver")
                  .trigger(availableNow=True) # process all records in the micro batch
                  .start()
          )

query.awaitTermination()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Unit tests that the customers_silver total records match the customer_id distinct records, if yes then we dont have any id duplicates and there is no ranking

# COMMAND ----------

count = spark.table("customers_silver").count()
expected_count = spark.table("customers_silver").select("customer_id").distinct().count()

assert count == expected_count
print("Unit test passed.")

# COMMAND ----------


