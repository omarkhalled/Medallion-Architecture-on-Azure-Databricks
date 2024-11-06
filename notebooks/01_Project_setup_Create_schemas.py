# Databricks notebook source
# MAGIC %md
# MAGIC ### Define Schema Paths

# COMMAND ----------

# MAGIC %run "/Users/omar_20180367@fci.helwan.edu.eg/04_common"

# COMMAND ----------

dbutils.widgets.text(name="env",defaultValue="",label=" Enter the environment in lower case")
env = dbutils.widgets.get("env")

# COMMAND ----------

print(env)

# COMMAND ----------

# MAGIC %md
# MAGIC # Create Schemas

# COMMAND ----------

def create_bronze_schema(environment,path):
    print(f"Using {environment}_catalog")
    spark.sql(f"""use catalog `{environment}_catalog`""")
    print(f"Creating Bronze Schema in {environment}_catalog")
    spark.sql(f"""
    CREATE SCHEMA IF NOT EXISTS `bronze` 
    MANAGED LOCATION '{path}/bronze'
""")


# COMMAND ----------

def create_silver_schema(environment,path):
    print(f"Using {environment}_catalog")
    spark.sql(f"""use catalog `{environment}_catalog`""")
    print(f"Creating Silver Schema in {environment}_catalog")
    spark.sql(f"""
    CREATE SCHEMA IF NOT EXISTS `silver` 
    MANAGED LOCATION '{path}/silver'
""")


# COMMAND ----------

def create_gold_schema(environment,path):
    print(f"Using {environment}_catalog")
    spark.sql(f"""use catalog `{environment}_catalog`""")
    print(f"Creating Gold Schema in {environment}_catalog")
    spark.sql(f"""
    CREATE SCHEMA IF NOT EXISTS `gold` 
    MANAGED LOCATION '{path}/gold'
""")


# COMMAND ----------

# MAGIC %md
# MAGIC # Create Bronze Tables

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Raw Traffic Table

# COMMAND ----------

def createTable_rawTraffic(environment):
    print(f'Creating raw_Traffic table in {environment}_catalog')
    spark.sql(f"""CREATE TABLE IF NOT EXISTS `{environment}_catalog`.`bronze`.`raw_traffic`
                        (
                            Record_ID INT,
                            Count_point_id INT,
                            Direction_of_travel VARCHAR(255),
                            Year INT,
                            Count_date VARCHAR(255),
                            hour INT,
                            Region_id INT,
                            Region_name VARCHAR(255),
                            Local_authority_name VARCHAR(255),
                            Road_name VARCHAR(255),
                            Road_Category_ID INT,
                            Start_junction_road_name VARCHAR(255),
                            End_junction_road_name VARCHAR(255),
                            Latitude DOUBLE,
                            Longitude DOUBLE,
                            Link_length_km DOUBLE,
                            Pedal_cycles INT,
                            Two_wheeled_motor_vehicles INT,
                            Cars_and_taxis INT,
                            Buses_and_coaches INT,
                            LGV_Type INT,
                            HGV_Type INT,
                            EV_Car INT,
                            EV_Bike INT,
                            Extract_Time TIMESTAMP
                    );""")
    
    print("************************************")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Raw Road Table

# COMMAND ----------

def createTable_rawRoad(environment):
    print(f'Creating raw_roads table in {environment}_catalog')
    spark.sql(f"""CREATE TABLE IF NOT EXISTS `{environment}_catalog`.`bronze`.`raw_roads`
                        (
                            Road_ID INT,
                            Road_Category_Id INT,
                            Road_Category VARCHAR(255),
                            Region_ID INT,
                            Region_Name VARCHAR(255),
                            Total_Link_Length_Km DOUBLE,
                            Total_Link_Length_Miles DOUBLE,
                            All_Motor_Vehicles DOUBLE,
                            Extract_Time TIMESTAMP
                    );""")
    
    print("************************************")

# COMMAND ----------

# MAGIC %md
# MAGIC # Call Functions

# COMMAND ----------

create_bronze_schema(env,bronze_path)
createTable_rawTraffic(env)
createTable_rawRoad(env)
create_silver_schema(env,silver_path)
create_gold_schema(env,gold_path)
