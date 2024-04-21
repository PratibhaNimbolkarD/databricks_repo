# Databricks notebook source
from pyspark.sql.types import StructType,StructField,IntegerType,StringType

# COMMAND ----------

employee_schema = StructType([
    StructField("Emp_Id", IntegerType(), True),
    StructField("Emp_Name", StringType(), True),
    StructField("department", StringType(), True),
    StructField("country", StringType(), True),
    StructField("salary", IntegerType(), True),
    StructField("age", IntegerType(), True)
])

# COMMAND ----------

department_schema = StructType([
    StructField("DepartmentID", StringType(), True),
    StructField("DepartmentName", StringType(), True)
])

# COMMAND ----------

country_schema = StructType([
    StructField("CountryCode", StringType(), True),
    StructField("CountryName", StringType(), True)
])