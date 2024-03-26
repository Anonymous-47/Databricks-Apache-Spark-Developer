# Databricks notebook source
users_list = [(1,'Ankit'),(2,'Gourav'),(3,'Abhay'),(4,'Atul')]

# COMMAND ----------

from pyspark.sql import Row

# COMMAND ----------

display(spark.createDataFrame(users_list,['user_id','user_first_name']))

# COMMAND ----------

users_rows = [Row(*user) for user in users_list]
print(users_rows)

# COMMAND ----------

display(spark.createDataFrame(users_rows))