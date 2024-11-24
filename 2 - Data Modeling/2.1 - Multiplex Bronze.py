# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div  style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://raw.githubusercontent.com/derar-alhussein/Databricks-Certified-Data-Engineer-Professional/main/Includes/images/bronze.png" width="60%">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ### /////////////////////////  Ignore, this is for CLI tests ////////////////////////////////

# COMMAND ----------

dbutils.fs.ls(f"dbfs:/mnt/uploads/")

# COMMAND ----------

# MAGIC %md
# MAGIC ### ///////////////////////////////////////////////////////////////////////////////////////

# COMMAND ----------

db_password = dbutils.secrets.get(scope="thameur", key="db_password")

# COMMAND ----------

# MAGIC %run ../Includes/Copy-Datasets

# COMMAND ----------

files = dbutils.fs.ls(f"{dataset_bookstore}/kafka-raw")
display(files)

# COMMAND ----------

df_raw = spark.read.json(f"{dataset_bookstore}/kafka-raw")
display(df_raw)

# COMMAND ----------

from pyspark.sql import functions as F

def process_bronze():
  
    schema = "key BINARY, value BINARY, topic STRING, partition LONG, offset LONG, timestamp LONG" # schema of the table

    query = (spark.readStream
                        .format("cloudFiles") # this is Autoloader
                        .option("cloudFiles.format", "json") # Autoloader uses json
                        .schema(schema)
                        .load(f"{dataset_bookstore}/kafka-raw")
                        .withColumn("timestamp", (F.col("timestamp")/1000).cast("timestamp"))  # human readable timestamp
                        .withColumn("year_month", F.date_format("timestamp", "yyyy-MM")) # extract only yyyy-MM
                  .writeStream
                      .option("checkpointLocation", "dbfs:/mnt/demo_pro/checkpoints/bronze")
                      .option("mergeSchema", True) # Levrage the schema evolution of Autoloader
                      .partitionBy("topic", "year_month") # we partition the table by topic and year_month: this is a good partition since it is not too much granular
                      .trigger(availableNow=True) # process all avaialable data and stop!
                      .table("bronze"))
    
    query.awaitTermination()

# COMMAND ----------

process_bronze()

# COMMAND ----------

batch_df = spark.table("bronze")
display(batch_df)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM bronze

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT(topic)
# MAGIC FROM bronze

# COMMAND ----------

bookstore.load_new_data() # copy new datafiles to source directory

# COMMAND ----------

process_bronze()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM bronze
