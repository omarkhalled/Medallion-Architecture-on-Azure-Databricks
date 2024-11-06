# Databricks notebook source
# MAGIC %md
# MAGIC # Re using functions and variables
# MAGIC - Removing Duplicates
# MAGIC - Handling Nulls

# COMMAND ----------

checkpoint = spark.sql("describe external location `checkpoints`").select("url").collect()[0][0]
landing = spark.sql("describe external location `landing`").select("url").collect()[0][0]
bronze = spark.sql("describe external location `bronze`").select("url").collect()[0][0]
silver = spark.sql("describe external location `silver`").select("url").collect()[0][0]
gold = spark.sql("describe external location `gold`").select("url").collect()[0][0]

# COMMAND ----------

# MAGIC %md
# MAGIC ### Remove duplicates

# COMMAND ----------

def remove_duplicates(df):
    print("****Removing Duplicates****")
    df = df.dropDuplicates()
    print('Duplicates Removed!! ')
    return df


# COMMAND ----------

# MAGIC %md
# MAGIC ### Handling null values

# COMMAND ----------

from pyspark.sql import functions as F

def handle_null_values(df):
    print("****Handling NULL Values****")
    print("Replacing NULL values on String Columns with 'Unknown'")
    df = df.fillna('Unknown')
    print('Success!! ')

    print("Replacing NULL values on Numeric Columns with 0")
    df = df.fillna(0)
    print('Success!! ')
    
    return df


# COMMAND ----------


