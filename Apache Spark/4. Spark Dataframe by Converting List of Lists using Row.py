# Databricks notebook source
users_list = [[1,'Ankit'],[2,'Gourav'],[3,'Abhay'],[4,'Atul']]

# COMMAND ----------

# For a single column list schema is mandatory i.e [1,2,3,4]. But for list of lists the schema isn't mandatory as shown below
df = spark.createDataFrame(users_list)
display(df)

# COMMAND ----------

df = spark.createDataFrame(users_list,'user_id int,user_first_name string')
display(df)

# COMMAND ----------

from pyspark.sql import Row

# COMMAND ----------

df = []
for i in users_list:
    df.append(Row(*i))
print(df)

# COMMAND ----------

# Using list comprehension
# When we pass * it means we are passing varying arguments
users_rows = [Row(*user) for user in users_list]
users_rows

# COMMAND ----------

display(spark.createDataFrame(users_rows,['user_id','user_first_name']))

# COMMAND ----------

user_details = [1,'Gourav']

# COMMAND ----------

def dummy(*args):
    print(args)

# COMMAND ----------

dummy(user_details)

# COMMAND ----------

dummy(*user_details)