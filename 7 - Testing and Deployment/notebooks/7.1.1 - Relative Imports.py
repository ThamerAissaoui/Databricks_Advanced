# Databricks notebook source
# MAGIC %run ./helpers/cube_notebook

# COMMAND ----------

c1 = Cube(3)
c1.get_volume()

# COMMAND ----------

# MAGIC %md
# MAGIC ### cannot import notebook

# COMMAND ----------

from helpers.cube_notebook import Cube

# COMMAND ----------

# MAGIC %md
# MAGIC ### but we can import python files

# COMMAND ----------

from helpers.cube import Cube_PY

# COMMAND ----------

c2 = Cube_PY(3)
c2.get_volume()

# COMMAND ----------

# MAGIC %md
# MAGIC ### getting the working dir

# COMMAND ----------

# MAGIC %sh pwd

# COMMAND ----------

# MAGIC %md
# MAGIC ### listing the files in current dir

# COMMAND ----------

# MAGIC %sh ls ./helpers

# COMMAND ----------

# MAGIC %md
# MAGIC ### list all dirs

# COMMAND ----------

import sys

for path in sys.path:
    print(path)

# COMMAND ----------

# MAGIC %md
# MAGIC ### adding the modules dir to all dirs

# COMMAND ----------

import os
sys.path.append(os.path.abspath('../modules'))

# COMMAND ----------

for path in sys.path:
    print(path)

# COMMAND ----------

# MAGIC %md
# MAGIC ### importing code from the modules dir

# COMMAND ----------

from shapes.cube import Cube as CubeShape

# COMMAND ----------

c3 = CubeShape(3)
c3.get_volume()

# COMMAND ----------

# MAGIC %md
# MAGIC ### importing wheels, wheels are like jars for java

# COMMAND ----------

# MAGIC %md
# MAGIC this one contains the same code cube

# COMMAND ----------

# MAGIC %pip install ../wheels/shapes-1.0.0-py3-none-any.whl

# COMMAND ----------

from shapes_wheel.cube import Cube as Cube_WHL

# COMMAND ----------

c4 = Cube_WHL(3)
c4.get_volume()

# COMMAND ----------

# MAGIC %md
# MAGIC ### this is executed on all the nodes of the active cluster 

# COMMAND ----------

# MAGIC %pip install ../wheels/shapes-1.0.0-py3-none-any.whl

# COMMAND ----------

# MAGIC %md
# MAGIC ### This is executed on the local driver machine

# COMMAND ----------

#%sh pip install ../wheels/shapes-1.0.0-py3-none-any.whl
