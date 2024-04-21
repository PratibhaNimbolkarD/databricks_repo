# Databricks notebook source
# MAGIC %run /Users/pratibha.nimbolkar@diggibyte.com/assignment1/source_to_bronze/utils

# COMMAND ----------

employee_df = spark.read.format("delta").load('dbfs:/FileStore/assignments/questoin1/silver/employee_info/dim_employee')


# COMMAND ----------

salary_of_each_department(employee_df)

# COMMAND ----------

employee_count(employee_df)

# COMMAND ----------

list_the_department(employee_df)

# COMMAND ----------

avg_age(employee_df)

# COMMAND ----------

add_load_date(employee_df)

# COMMAND ----------

employee_df.write.format("parquet").mode("overwrite").option("replaceWhere","load_date = '2024-04-16'").save("/FileStore/assignments/gold/employee/table_name")

# COMMAND ----------

