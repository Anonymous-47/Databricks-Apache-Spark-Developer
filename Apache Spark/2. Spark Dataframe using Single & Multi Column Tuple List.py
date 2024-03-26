# Databricks notebook source
age_list = [(21,),(23,),(41,),(32,)]

# COMMAND ----------

type(age_list[0])

# COMMAND ----------

display(spark.createDataFrame(age_list))

# COMMAND ----------

display(spark.createDataFrame(age_list,'age int'))

# COMMAND ----------

users_list = [(1,'Ankit'),(2,'Gourav'),(3,'Abhay'),(4,'Atul')]

# COMMAND ----------

display(spark.createDataFrame(users_list,'user_id int, user_first_name string'))