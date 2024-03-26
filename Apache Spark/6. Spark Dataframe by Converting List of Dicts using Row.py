# Databricks notebook source
users_list = [{'user_id':1,'user_first_name':'Ankit'},
              {'user_id':2,'user_first_name':'Gourav'},
              {'user_id':3,'user_first_name':'Abhay'},
              {'user_id':4,'user_first_name':'Atul'}]

# COMMAND ----------

display(spark.createDataFrame(users_list))

# COMMAND ----------

from pyspark.sql import Row

# COMMAND ----------

for i in range(0,len(users_list)):
    print(Row(**users_list[i]))

# COMMAND ----------

user_rows = [Row(**users_list[i]) for i in range(0,len(users_list))]
user_rows

# COMMAND ----------

user_rows = [Row(**users) for users in (users_list)]
user_rows

# COMMAND ----------

display(spark.createDataFrame(user_rows))

# COMMAND ----------

for user_dict in users_list:
    print(Row(*user_dict.values()))
    # print(Row(*user_dict.keys()))

# COMMAND ----------

user_rows = [Row(*user_dict.values()) for user_dict in users_list]
user_rows

# COMMAND ----------

display(spark.createDataFrame(user_rows,'user_id int,user_first_name string'))

# COMMAND ----------

