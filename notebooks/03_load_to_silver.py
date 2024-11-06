# Databricks notebook source
# MAGIC %run "/Users/omar_20180367@fci.helwan.edu.eg/04_common"

# COMMAND ----------

dbutils.widgets.text(name="env",defaultValue='',label='Enter the environment in lower case')
env = dbutils.widgets.get("env")

# COMMAND ----------

def read_BronzeTrafficTable(environment):
    print("Reading the Bronze Table Data")
    df_bronzeTraffic = (spark.readStream
                    .table(f"`{environment}_catalog`.`bronze`.raw_traffic")
                    )
    print(f'Reading {environment}_catalog.bronze.raw_traffic Success!')
    return df_bronzeTraffic

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.functions import current_timestamp

def process_traffics_data(df):
    print("****Processing Roads Data****")

    # Create Electric Vehicles Count Column
    print('Creating Electric Vehicles Count Column')
    df = df.withColumn('Electric_Vehicles_Count',
                        F.col('EV_Car') + F.col('EV_Bike'))
    print('Success!! ')

    # Create All Motor Vehicles Count Column
    print('Creating All Motor Vehicles Count Column')
    df = df.withColumn('Motor_Vehicles_Count',
                        F.col('Electric_Vehicles_Count') +
                        F.col('Two_wheeled_motor_vehicles') +
                        F.col('Cars_and_taxis') +
                        F.col('Buses_and_coaches') +
                        F.col('LGV_Type') +
                        F.col('HGV_Type'))
    print('Success!! ')

    # Add a Quality Check Status Column
    df = df.withColumn("Quality_Check", F.lit("Passed"))
    
    # Add a Transformed Time Column
    df = df.withColumn("Transformed_Time", current_timestamp())

    # Fix "Count_date" Date Format
    df = df.withColumn(
        "Count_date",
        F.to_date(F.to_timestamp(F.col("Count_date"), "M/d/yyyy H:mm"))  # Flexible format for 1 or 2 digits
    )

    print("****Processing Completed****")
    return df


# COMMAND ----------

def read_BronzeRoadsTable(environment):
    print("Reading the Bronze Roads Table Data")
    df_bronzeRoads = (spark.readStream
                    .table(f"`{environment}_catalog`.`bronze`.raw_roads")
                    )
    print(f'Reading {environment}_catalog.bronze.raw_roads Success!')
    return df_bronzeRoads

# COMMAND ----------

def road_Category(df):
    print('Creating Road Category Name Column: ', end='')
    from pyspark.sql.functions import when,col

    df_road_Cat = df.withColumn("Road_Category_Name",
                  when(col('Road_Category') == 'TA', 'Class A Trunk Road')
                  .when(col('Road_Category') == 'TM', 'Class A Trunk Motor')
                   .when(col('Road_Category') == 'PA','Class A Principal road')
                    .when(col('Road_Category') == 'PM','Class A Principal Motorway')
                    .when(col('Road_Category') == 'M','Class B road')
                    .otherwise('NA')
                  
                  )
    print('Success!! ')
    print('***********************')
    return df_road_Cat

# COMMAND ----------

def road_Type(df):
    print('Creating Road Type Name Column: ', end='')
    from pyspark.sql.functions import when,col

    df_road_Type = df.withColumn("Road_Type",
                  when(col('Road_Category_Name').like('%Class A%'),'Major')
                  .when(col('Road_Category_Name').like('%Class B%'),'Minor')
                    .otherwise('NA')
                  
                  )
    print('Success!! ')
    print('***********************')
    return df_road_Type

# COMMAND ----------

def write_Roads_SilverTable(StreamingDF,environment):
    print('Writing the silver_roads Data : ',end='') 

    write_StreamSilver_R = (StreamingDF.writeStream
                .format('delta')
                .option('checkpointLocation',checkpoint+ "/SilverRoadsLoad/Checkpt/")
                .outputMode('append')
                .queryName("SilverRoadsWriteStream")
                .trigger(availableNow=True)
                .toTable(f"`{environment}_catalog`.`silver`.`silver_roads`"))
    
    write_StreamSilver_R.awaitTermination()
    print(f'Writing `{environment}_catalog`.`silver`.`silver_roads` Success!')

# COMMAND ----------

def write_Traffic_SilverTable(StreamingDF,environment):
    print("Writing the silver_traffic Data") 

    write_StreamSilver = (StreamingDF.writeStream
                .format('delta')
                .option('checkpointLocation',checkpoint+ "/SilverTrafficLoad/Checkpt/")
                .outputMode('append')
                .queryName("SilverTrafficWriteStream")
                .trigger(availableNow=True)
                .toTable(f"`{environment}_catalog`.`silver`.`silver_traffic`"))
    
    write_StreamSilver.awaitTermination()
    print(f'Writing `{environment}_catalog`.`silver`.`silver_traffic` Success!')

# COMMAND ----------

df_bronzeTraffic = read_BronzeTrafficTable(env)
df_dup = remove_duplicates(df_bronzeTraffic)
df_null = handle_null_values(df_dup)
process_traffics_data(df_null)


df_bronzeRoads = read_BronzeRoadsTable(env)
df_Roadsdup = remove_duplicates(df_bronzeRoads)
df_Roadsnull = handle_null_values(df_Roadsdup)
df_roadCat = road_Category(df_Roadsnull)
df_type = road_Type(df_roadCat)
write_Roads_SilverTable(df_type,env)


# COMMAND ----------

display(spark.sql("select * from `dev_catalog`.`silver`.`silver_traffic`"))

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from `dev_catalog`.`silver`.`silver_traffic` where Record_ID > 37090

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM `dev_catalog`.`silver`.`silver_roads` limit 50
