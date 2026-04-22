# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "b7fb114b-07b5-4f0a-aeb7-dd6bb681c5a7",
# META       "default_lakehouse_name": "GetStartedDataFactoryTutorial",
# META       "default_lakehouse_workspace_id": "8d919179-54dc-4df2-8d9f-940230d115c3",
# META       "known_lakehouses": [
# META         {
# META           "id": "b7fb114b-07b5-4f0a-aeb7-dd6bb681c5a7"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC SELECT *
# MAGIC FROM salesLT.Customer
# MAGIC WHERE CustomerID = 10000
# MAGIC 


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
