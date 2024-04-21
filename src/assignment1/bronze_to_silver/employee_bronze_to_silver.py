# Databricks notebook source
# MAGIC %run /Users/pratibha.nimbolkar@diggibyte.com/assignment1/source_to_bronze/utils
# MAGIC

# COMMAND ----------

# MAGIC %run /Users/pratibha.nimbolkar@diggibyte.com/assignment1/source_to_bronze/schema

# COMMAND ----------



emp_path = f'dbfs:/FileStore/assignments/assign1/source_to_bronze/employee.csv'
employee_df = custom_schema_read_csv(emp_path , schema= employee_schema)
employee_df.show()




# COMMAND ----------



# COMMAND ----------

dep_path = 'dbfs:/FileStore/assignments/assign1/source_to_bronze/department.csv/part-00000-tid-2882255029493062016-d1ad3e95-7576-478f-8622-a6c4dd54a8c7-34-1-c000.csv'
department_df = custom_schema_read_csv(dep_path, department_schema)
department_df.show()


# COMMAND ----------

country_path = 'dbfs:/FileStore/assignments/assign1/source_to_bronze/country.csv/part-00000-tid-1152614239620934396-b87d3132-1b94-40d1-8bd1-bd170c96fc85-35-1-c000.csv'
country_df = spark.read.csv(country_path,country_schema)
country_df.printSchema()
country_df.show()

# COMMAND ----------

employee_snake_case = camel_to_snake(employee_df)
display(employee_snake_case)



# COMMAND ----------

date_load = current_date_df(employee_snake_case)
display(date_load)

# COMMAND ----------


 spark.sql('CREATE DATABASE employee_info')
 spark.sql('use employee_info')



# COMMAND ----------

employee_df.write.option('path', 'dbfs:/FileStore/assignments/questoin1/silver/employee_info/dim_employee').saveAsTable('dim_employee')

# COMMAND ----------

