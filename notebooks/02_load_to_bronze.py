# Databricks notebook source
# MAGIC %run "/Users/omar_20180367@fci.helwan.edu.eg/04_common"

# COMMAND ----------

print(checkpoint)

# COMMAND ----------

dbutils.widgets.text(name="env",defaultValue='',label='Enter the environment in lower case')
env = dbutils.widgets.get("env")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reading functions

# COMMAND ----------

# MAGIC %md
# MAGIC ### read_traffic_data

# COMMAND ----------

def read_traffic_data():
    from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType,DateType
    from pyspark.sql.functions import current_timestamp
    print("****Reading Traffic Data****")
    schema = StructType([
        StructField("Record_ID",IntegerType()),
        StructField("Count_point_id",IntegerType()),
        StructField("Direction_of_travel",StringType()),
        StructField("Year",IntegerType()),
        StructField("Count_date",StringType()),
        StructField("hour",IntegerType()),
        StructField("Region_id",IntegerType()),
        StructField("Region_name",StringType()),
        StructField("Local_authority_name",StringType()),
        StructField("Road_name",StringType()),
        StructField("Road_Category_ID",IntegerType()),
        StructField("Start_junction_road_name",StringType()),
        StructField("End_junction_road_name",StringType()),
        StructField("Latitude",DoubleType()),
        StructField("Longitude",DoubleType()),
        StructField("Link_length_km",DoubleType()),
        StructField("Pedal_cycles",IntegerType()),
        StructField("Two_wheeled_motor_vehicles",IntegerType()),
        StructField("Cars_and_taxis",IntegerType()),
        StructField("Buses_and_coaches",IntegerType()),
        StructField("LGV_Type",IntegerType()),
        StructField("HGV_Type",IntegerType()),
        StructField("EV_Car",IntegerType()),
        StructField("EV_Bike",IntegerType())
    ])
    rawTraffic_stream = (spark.readStream.format("cloudFiles")
    .option("cloudFiles.format","csv")
    .option("cloudFiles.schemaLocation",f"{checkpoint}/rawTrafficLoad/schemaInfer")
    .option("header","true")
    .schema(schema)
    .load(landing+'/raw_traffic/')
    .withColumn("Extract_Time", current_timestamp()))
    print("****Reading success****")
    return rawTraffic_stream

# COMMAND ----------

# MAGIC %md
# MAGIC ### read_road_data

# COMMAND ----------

def read_roads_data():
    from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
    from pyspark.sql.functions import current_timestamp
    print("****Reading Roads Data****")
    schema = StructType([
            StructField('Road_ID',IntegerType()),
            StructField('Road_Category_Id',IntegerType()),
            StructField('Road_Category',StringType()),
            StructField('Region_ID',IntegerType()),
            StructField('Region_Name',StringType()),
            StructField('Total_Link_Length_Km',DoubleType()),
            StructField('Total_Link_Length_Miles',DoubleType()),
            StructField('All_Motor_Vehicles',DoubleType())
            
            ])
    rawRoads_stream = (spark.readStream.format("cloudFiles")
    .option("cloudFiles.format","csv")
    .option("cloudFiles.schemaLocation",f"{checkpoint}/rawRoadsLoad/schemaInfer")
    .option('header','true')
    .schema(schema)
    .load(landing+'/raw_roads/')
    .withColumn("Extract_Time", current_timestamp()))
    print("****Reading success****")
    return rawRoads_stream


# COMMAND ----------

# MAGIC %md
# MAGIC ## Writing Functions

# COMMAND ----------

# MAGIC %md
# MAGIC ### write_traffic_data

# COMMAND ----------

def write_traffic_data(streamingDf,environment):
    print("****write traffic data****")
    write_stream = (streamingDf.writeStream.format("delta")
    .option("checkpointLocation", f"{checkpoint}/rawTrafficLoad/checkpoint")
    .outputMode("append")
    .queryName('rawTrafficWriteStream')
    .trigger(availableNow=True)
    .toTable(f"`{environment}_catalog`.`bronze`.`raw_traffic`"))
    write_stream.awaitTermination()
    print("****writing success****")


# COMMAND ----------

# MAGIC %md
# MAGIC ### write_roads_data

# COMMAND ----------

def write_roads_data(streamingDf,environment):
    print("****write roads data****")
    write_stream = (streamingDf.writeStream.format("delta")
                    .option("checkpointLocation", f"{checkpoint}/rawRoadsLoad/checkpoint")
                    .outputMode("append")
                    .queryName('rawRoadsWriteStream')
                    .trigger(availableNow=True)
                    .toTable(f"`{environment}_catalog`.`bronze`.`raw_roads`"))
    write_stream.awaitTermination()
    print("****writing success****")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Calling functions

# COMMAND ----------

read_traffic= read_traffic_data()
write_traffic_data(read_traffic,env)
read_roads = read_roads_data()
write_roads_data(read_roads,env)

# COMMAND ----------

display(spark.sql(f"""SELECT * FROM `{env}_catalog`.`bronze`.`raw_traffic` LIMIT 10"""))


# COMMAND ----------

display(spark.sql(f"""SELECT * FROM `{env}_catalog`.`bronze`.`raw_roads` LIMIT 10"""))


# COMMAND ----------


