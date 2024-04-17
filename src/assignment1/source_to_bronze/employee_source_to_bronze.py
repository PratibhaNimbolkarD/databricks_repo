# Databricks notebook source
# MAGIC %run ./utils

# COMMAND ----------


emp_path = 'dbfs:/FileStore/resources/Employee_Q1.csv'
dep_path = 'dbfs:/FileStore/resources/Department_Q1.csv'
country_path = 'dbfs:/FileStore/resources/Country_Q1.csv'

employee_df = read_csv(emp_path)
department_df = read_csv(dep_path)
country_df = read_csv(country_path)

# COMMAND ----------

write_csv(employee_df, '/FileStore/assignments/assign1/source_to_bronze/employee.csv')
write_csv(department_df, '/FileStore/assignments/assign1/source_to_bronze/department.csv')
write_csv(country_df, '/FileStore/assignments/assign1/source_to_bronze/country.csv')


# COMMAND ----------

