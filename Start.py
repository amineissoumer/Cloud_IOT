# Databricks notebook source
# DBTITLE 1,Import
from pyspark.sql.functions import col, sum, mean

# COMMAND ----------

# DBTITLE 1,load the secret key to get access to shared blob storage
# Load into Spark the secret key so we can access later to access shared ressources
spark.conf.set("fs.azure.account.key.dhoyblobstorage1.blob.core.windows.net", "Pk3FiQdHwVYcks2TWOJ2zHus6twjrI7muGwn1qPJpD0L6ULEKmkJYWjve72SWPbQoJQiqaXiuNJLA+BfRFh4Qw==")

# COMMAND ----------

# DBTITLE 1,Load data: Dataset from INSE about population
# the present file come from the INSEE organisaiton and give population per city, in France in 2017, per range of age
# This is true data
df_FrenchPopulation = spark.read.format("csv")\
                                .option("header", "true")\
                                .option("delimiter", ",")\
                                .load("wasbs://containerpublic@dhoyblobstorage1.blob.core.windows.net/INSE_Population_2017.csv")

# we print the schema to check our data
df_FrenchPopulation.printSchema()
display(df_FrenchPopulation)

# COMMAND ----------

# DBTITLE 1,Challenge 1 : Change Type from string to int
# Data imported from CSV file are not typed (everything comes as a string), we need to convert all revelent column to int or double (since we count person, int is more approriate) 
# to be capable to do mathematical function later, as sum.
# --------------------------------------------------------------------------------------------------------------------------------------------


# COMMAND ----------

# DBTITLE 1,Challenge 2: Join extra data from a 2nd dataset
# Dataset delivered by INSEE use as reference the INSEE code for each city, but it is difficult it understand or use it, so we will 
# join data from a 2nd dataset where we have a mapping between CodeInsee and cityName.
# the file is nammed: MappingCodeInseeVSNomVille.csv and the blob storage is the same as the 1st file
# --------------------------------------------------------------------------------------------------------------------------------------------


# COMMAND ----------

# DBTITLE 1,Challenge 3: Display data for the city of Poitiers
# We want to visualize the data for the city of Poitiers
# --------------------------------------------------------------------------------------------------------------------------------------------


# COMMAND ----------

# DBTITLE 1,Challenge 4: Display all the city with population of retired person (60years or more) is higher than 100K
# We want to visualize the city where the retired people ( 60 year or more, both gender ) is higher than 100K
# --------------------------------------------------------------------------------------------------------------------------------------------


# COMMAND ----------

# DBTITLE 1,Challenge 5: Show the city of 10 K people or more with the higgest distortion between gender
# One of the KPI the most important for population statistic is to check the ratio:  Male VS Female - It has to be 1:1 
# We want to use our data to verify this and highlight city where this proportion is the most extreme: In our case, the city which is the ratio is too much male.
# --------------------------------------------------------------------------------------------------------------------------------------------


# COMMAND ----------

# DBTITLE 1,Callenge 6: Get the departement where this is highest number of young person 
# We want to know the number of young person ( =< 29 years, both gender) per departement and focus on top score departement
# --------------------------------------------------------------------------------------------------------------------------------------------


# COMMAND ----------

# DBTITLE 1,Challenge 7 : Get the most populated departement
# Finding the most populated departement can be evaluated by calculating the population of each city, then, for all city being part of the departement: getting the average.
# --------------------------------------------------------------------------------------------------------------------------------------------

