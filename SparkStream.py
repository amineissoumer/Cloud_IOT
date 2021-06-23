# Databricks notebook source
# DBTITLE 1,Initial import
from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

# DBTITLE 1,Initialise event hub
# By default, Event hub is not supported natively so following package has to be installed:
# MAVEN com.microsoft.azure:azure-eventhubs-spark_2.12:2.3.17  for a Databrick version 7.4

# Here we expect the connection string as returned by MS Azure durign the creation of the SAS access policiy:
# Expected: Something like: "Endpoint=sb://eventhubstest.servicebus.windows.net/;SharedAccessKeyName=SASPolicy_ReadFromEventHub;SharedAccessKey=fXPlWuGxXTOIHBzFEdQ0VTxZvWuDpyy7dJqDk=;EntityPath=channel1"
CONNECTION_STRING = "Endpoint=sb://eventhubstest.servicebus.windows.net/;SharedAccessKeyName=SASPolicy_ReadFromEventHub;SharedAccessKey=fXPlWuGxXTOIHBzFEdQ0VTxZvWuDpyy7dJqDk=;EntityPath=channel1"

 #Spark need to get the endpoint connection string in an encrypted way (note: this is poorly documented)
ehConf = { 'eventhubs.connectionString' : sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(CONNECTION_STRING), 
           'eventhubs.consumerGroup' : "$Default"
         }

ds_EventHubRaw = spark.readStream.format("eventhubs").options(**ehConf).load()

ds_EventHubRaw.printSchema()
display(ds_EventHubRaw, processingTime = "5 seconds")

# COMMAND ----------

# DBTITLE 1,See un-structured data (AKA raw)
# we need to change the type of the body (being binary) into a string (which will actually host a JSON payload)
ds_EventHub = ds_EventHubRaw.withColumn("JSON", col("body").cast("string"))

# we consider only these 2 column to have valuable information: To ease the visualisation, we order most recent 1st
ds_EventHub = ds_EventHub.select("JSON","enqueuedTime")

# See the data
ds_EventHub.printSchema()
display(ds_EventHub, processingTime = "5 seconds")

# COMMAND ----------

# DBTITLE 1,Make the data structured: Parse the JSON
DataSchema = StructType([
  StructField("SensorName", StringType(), True),
  StructField("TimeStamp", DoubleType(), True),
  StructField("TemperatureValue", DoubleType(), True)])

ds_DecodedData = ds_EventHub.withColumn("Payload", from_json(col("JSON"), DataSchema))

ds_DecodedData.printSchema()
display(ds_DecodedData, processingTime = "5 seconds")

# COMMAND ----------

# DBTITLE 1,Make the data structured: Part 2
ds_DecodedData = ds_DecodedData.select("Payload.*")
ds_DecodedData = ds_DecodedData.withColumn("TimeStamp", to_timestamp(col('TimeStamp')))

ds_DecodedData.printSchema()
display(ds_DecodedData, processingTime = "5 seconds")

# COMMAND ----------

# DBTITLE 1,Use user prefered name ISO technical name
# In production environement, such data comes from a database but for simplicity reason, 
# it has been decided to statically create a dummy dataset in the actual case.
df_UserSettings    =  spark.createDataFrame([("Sensor0","Kitchen"),
                                             ("Sensor1","Livingroom"),
                                             ("Sensor2","BedRoom1"),
                                             ("Sensor3","BedRoom2"),
                                             ("Sensor4","ExtraRoom")],
                                              StructType ([
                                              StructField("SensorName", StringType(), True),
                                              StructField("Location", StringType(), True)]))                                

# we left join the stream with a static dataset.
ds_DecodedData = ds_DecodedData.join(df_UserSettings,"SensorName", how='left').drop("SensorName")

display(ds_DecodedData)

# COMMAND ----------

# DBTITLE 1,Aggregate data: Display: min, avg, max per slice of 1min
ds_AggNbData = ds_DecodedData.groupBy(window("TimeStamp", "1 minute")).agg(min('TemperatureValue'),mean('TemperatureValue'),max('TemperatureValue')).sort(col("window.start").desc())

display(ds_AggNbData, processingTime = "5 seconds")
