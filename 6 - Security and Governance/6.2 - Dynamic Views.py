# Databricks notebook source
# MAGIC %md
# MAGIC ### Dynamic views allow AC to be applied to a data ina table at the column or row level, users with sufficient previleges will be able to see all the fields, other users will have restricted rights

# COMMAND ----------

# MAGIC %run ../Includes/Copy-Datasets

# COMMAND ----------

# MAGIC %sql 
# MAGIC DESCRIBE TABLE customers_silver

# COMMAND ----------

# MAGIC %md
# MAGIC ### redacted view to hide PII to some users

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW customers_vw AS
# MAGIC   SELECT
# MAGIC     customer_id,
# MAGIC     CASE 
# MAGIC       WHEN is_member('admins_demo') THEN email
# MAGIC       ELSE 'REDACTED'
# MAGIC     END AS email,
# MAGIC     gender,
# MAGIC     CASE 
# MAGIC       WHEN is_member('admins_demo') THEN first_name
# MAGIC       ELSE 'REDACTED'
# MAGIC     END AS first_name,
# MAGIC     CASE 
# MAGIC       WHEN is_member('admins_demo') THEN last_name
# MAGIC       ELSE 'REDACTED'
# MAGIC     END AS last_name,
# MAGIC     CASE 
# MAGIC       WHEN is_member('admins_demo') THEN street
# MAGIC       ELSE 'REDACTED'
# MAGIC     END AS street,
# MAGIC     city,
# MAGIC     country,
# MAGIC     row_time
# MAGIC   FROM customers_silver

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM customers_vw

# COMMAND ----------

# MAGIC %md
# MAGIC ### row level

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW customers_fr_vw AS
# MAGIC SELECT * FROM customers_vw
# MAGIC WHERE 
# MAGIC   CASE 
# MAGIC     WHEN is_member('admins_demo') THEN TRUE -- see everything
# MAGIC     ELSE country = "France" AND row_time > "2022-01-01" -- only see France since 2022-01-01
# MAGIC   END

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM customers_fr_vw
