# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "d8a0fbfa-edc2-438a-95eb-11c37171c532",
# META       "default_lakehouse_name": "GoldLayer",
# META       "default_lakehouse_workspace_id": "f70b78b0-968f-4c16-82df-35b50fbb2937",
# META       "known_lakehouses": [
# META         {
# META           "id": "d8a0fbfa-edc2-438a-95eb-11c37171c532"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC Describe history GoldLayer.DimCustomer

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.read.format("delta").option("versionAsOf", 14).load('abfss://WorldWideImporters@onelake.dfs.fabric.microsoft.com/GoldLayer.Lakehouse/Tables/DimCustomer')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
