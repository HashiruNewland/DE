# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "659418a7-6453-493c-bba1-3c0bc50de98c",
# META       "default_lakehouse_name": "MSLearnDEnb",
# META       "default_lakehouse_workspace_id": "cbfd01ac-d3b6-4081-bcbb-7920774a3fd8",
# META       "known_lakehouses": [
# META         {
# META           "id": "659418a7-6453-493c-bba1-3c0bc50de98c"
# META         }
# META       ]
# META     }
# META   }
# META }

# PARAMETERS CELL ********************

a = 10
b = 20

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM MSLearnDEnb.dbo.fact_sale LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
