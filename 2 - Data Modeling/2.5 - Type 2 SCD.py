# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div  style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://raw.githubusercontent.com/derar-alhussein/Databricks-Certified-Data-Engineer-Professional/main/Includes/images/books.png" width="60%">
# MAGIC </div>

# COMMAND ----------

# MAGIC %run ../Includes/Copy-Datasets

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from updates

# COMMAND ----------

# MAGIC %sql
# MAGIC -- NOTE: This cell contains the SQL query for your reference, and won't work if run directly.
# MAGIC -- The query is used below in the type2_upsert() function as part of the foreachBatch call.
# MAGIC
# MAGIC MERGE INTO books_silver
# MAGIC USING (
# MAGIC     SELECT updates.book_id as merge_key, updates.*
# MAGIC     FROM updates
# MAGIC
# MAGIC     UNION ALL
# MAGIC
# MAGIC     SELECT NULL as merge_key, updates.*
# MAGIC     FROM updates
# MAGIC     JOIN books_silver ON updates.book_id = books_silver.book_id
# MAGIC     WHERE books_silver.current = true AND updates.price <> books_silver.price
# MAGIC   ) staged_updates
# MAGIC ON books_silver.book_id = merge_key 
# MAGIC WHEN MATCHED AND books_silver.current = true AND books_silver.price <> staged_updates.price THEN
# MAGIC   UPDATE SET current = false, end_date = staged_updates.updated
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT (book_id, title, author, price, current, effective_date, end_date)
# MAGIC   VALUES (staged_updates.book_id, staged_updates.title, staged_updates.author, staged_updates.price, true, staged_updates.updated, NULL)

# COMMAND ----------

def type2_upsert(microBatchDF, batchId):
    # Register the incoming micro-batch DataFrame as a temporary view
    microBatchDF.createOrReplaceTempView("updates")
    
    # Define the SQL query for SCD Type 2 logic
    sql_query = """
        MERGE INTO books_silver AS target
        USING (
            SELECT
                updates.book_id,
                updates.title,
                updates.author,
                updates.price,
                updates.updated AS effective_date
            FROM updates
            LEFT JOIN books_silver AS current_books
                ON updates.book_id = current_books.book_id
                AND current_books.current = true
            WHERE current_books.book_id IS NULL OR updates.price <> current_books.price
        ) AS staged_updates
        ON target.book_id = staged_updates.book_id
            AND target.current = true
        WHEN MATCHED THEN
            UPDATE SET 
                current = false,
                end_date = staged_updates.effective_date
        WHEN NOT MATCHED THEN
            INSERT (book_id, title, author, price, current, effective_date, end_date)
            VALUES (staged_updates.book_id, staged_updates.title, staged_updates.author, staged_updates.price, true, staged_updates.effective_date, NULL)
    """
    
    # Execute the SQL query
    microBatchDF.sparkSession.sql(sql_query)


# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS books_silver
# MAGIC (book_id STRING, title STRING, author STRING, price DOUBLE, current BOOLEAN, effective_date TIMESTAMP, end_date TIMESTAMP)

# COMMAND ----------

from pyspark.sql.functions import from_json, col

# Function to process books data and apply SCD Type 2
def process_books():
    # Define the schema of the JSON data
    schema = """
        book_id STRING, 
        title STRING, 
        author STRING, 
        price DOUBLE, 
        updated TIMESTAMP
    """
    
    # Define the streaming query
    query = (
        spark.readStream
        .table("bronze")  # Read from the Bronze table as a streaming source
        .filter("topic = 'books'")  # Filter data specific to the 'books' topic
        .select(from_json(col("value").cast("string"), schema).alias("v"))  # Parse JSON and apply schema
        .select("v.*")  # Flatten the parsed JSON into separate columns
        .writeStream
        .foreachBatch(type2_upsert)  # Apply SCD Type 2 logic in the upsert function
        .option("checkpointLocation", "dbfs:/mnt/demo_pro/checkpoints/books_silver")  # Specify checkpoint location
        .trigger(availableNow=True)  # Process all available data in the stream
        .start()
    )
    
    # Wait for the streaming query to finish
    query.awaitTermination()

# Run the process
process_books()


# COMMAND ----------

books_df = spark.read.table("books_silver").orderBy("book_id", "effective_date")
display(books_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Process new books

# COMMAND ----------

bookstore.load_books_updates()
bookstore.process_bronze()
process_books()

# COMMAND ----------

books_df = spark.read.table("books_silver").orderBy("book_id", "effective_date")
display(books_df)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC <div  style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://raw.githubusercontent.com/derar-alhussein/Databricks-Certified-Data-Engineer-Professional/main/Includes/images/current_books.png" width="60%">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ### creating a table with only valid current data, no historical

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE current_books
# MAGIC AS SELECT book_id, title, author, price
# MAGIC    FROM books_silver
# MAGIC    WHERE current IS TRUE

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM current_books
# MAGIC ORDER BY book_id
