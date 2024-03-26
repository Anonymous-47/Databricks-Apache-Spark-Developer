# Databricks notebook source
users_list = [(1,'Ankit'),(2,'Gourav'),(3,'Abhay'),(4,'Atul')]

# COMMAND ----------

df = spark.createDataFrame(users_list,'user_id int, user_first_name string')

# COMMAND ----------

df.show()

# COMMAND ----------

# Spark Dataframe is a collection of Row objects
# Collect method converts dataframe to a python list
df.collect()

# COMMAND ----------

type(df.collect())

# COMMAND ----------

from pyspark.sql import Row

# COMMAND ----------

help(Row)

# COMMAND ----------

#Varying list of arguments (*args)
r1 = Row("Alice", 11)

# COMMAND ----------

r1[0]

# COMMAND ----------

#Varying list of keyword arguments (*kargs)
r2 = Row(name="Alice", age=11)

# COMMAND ----------

r2.name

# COMMAND ----------

r2['name']