# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "cf6e5417-b842-4953-bd4a-8376d9e42932",
# META       "default_lakehouse_name": "MaterializedLakeView",
# META       "default_lakehouse_workspace_id": "8d919179-54dc-4df2-8d9f-940230d115c3",
# META       "known_lakehouses": [
# META         {
# META           "id": "cf6e5417-b842-4953-bd4a-8376d9e42932"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Create materialized lake views 
# 1. Use this notebook to create materialized lake views. 
# 2. Select **Run all** to run the notebook. 
# 3. When the notebook run is completed, return to your lakehouse and refresh your materialized lake views graph. 


# CELL ********************

# Welcome to your new notebook 
# Type here in the cell editor to add code! 
# Sample materialized lake view definition 

# import fmlv 
# @fmlv.materialized_lake_view (name="sample_mlv") 
# def sample_mlv(): 
#   return spark.read.table("sample_lh.bronze.table_name")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import fmlv 

@fmlv.materialized_lake_view(name="LH1.silver.customer_silver")
def customer_silver():
    df = spark.read.table("bronze.customer_bronze")
    cleaned_df = df.filter(F.col("sales").isNotNull())
    enriched_df = cleaned_df.withColumn("sales_in_usd", F.col("sales") * 1.0)
    return enriched_df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import fmlv

@fmlv.materialized_lake_view(
    name="LH1.silver.customer_enriched",
    partition_cols=["year", "city"],
    table_properties={"delta.enableChangeDataFeed": "true"}
)
def customer_enriched():
    df = spark.read.table("LH2.bronze.customer_bronze")
    cleaned_df = df.filter(F.col("sales").isNotNull())
    enriched_df = cleaned_df.withColumn("sales_in_usd", F.col("sales") * 1.0)
    return enriched_df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F 

# Simple helper function: concatenate columns as string 
def concat_name_age(df): 
    return df.withColumn( 
        "name_age", 
        F.concat(F.coalesce(F.col("name"), F.lit("")), 
                 F.lit("-"), 
                 F.coalesce(F.col("age").cast("string"), F.lit(""))) 
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import fmlv 

@fmlv.materialized_lake_view( 
    name="LH1.silver.customer_silver" 
) 
def customer_silver(): 
    # Read bronze table 
    bronze_df = spark.read.table("customer_bronze") 
    # Apply helper function 
    enriched_df = concat_name_age(bronze_df) 
    # Add uppercase name 
    enriched_df = enriched_df.withColumn("name_upper", F.upper(F.col("name"))) 
    return enriched_df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import fmlv

@fmlv.materialized_lake_view( 
    name="LH1.silver.customer_enriched" 
) 
def customer_enriched(): 
    df = spark.read.table("customer_bronze") 
    enriched_df = df.filter(F.col("sales").isNotNull()) 
    return enriched_df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
