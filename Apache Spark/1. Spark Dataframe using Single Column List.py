# Databricks notebook source
age_list = [21,23,18,41,32]

# COMMAND ----------

display(spark.createDataFrame(age_list,'int'))

# COMMAND ----------

from pyspark.sql.types import IntegerType,StringType

# COMMAND ----------

display(spark.createDataFrame(age_list,IntegerType()))

# COMMAND ----------

names_list = ['Sahil','Gourav','Ankit','Abhay','Ajay']

# COMMAND ----------

display(spark.createDataFrame(names_list,'string'))

# COMMAND ----------

display(spark.createDataFrame(names_list,StringType()))