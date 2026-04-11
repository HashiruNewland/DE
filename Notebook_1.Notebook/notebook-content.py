# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "jupyter",
# META     "jupyter_kernel_name": "python3.11"
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
# META     },
# META     "warehouse": {
# META       "default_warehouse": "47a749a1-b610-82eb-45ed-6355d2c827e1",
# META       "known_warehouses": [
# META         {
# META           "id": "47a749a1-b610-82eb-45ed-6355d2c827e1",
# META           "type": "Datawarehouse"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# MAGIC %%tsql -artifact AddDWandSQLendpointToAnOtebook -type Warehouse -bind df1
# MAGIC SELECT TOP (100) [DateID],
# MAGIC 			[MedallionID],
# MAGIC 			[HackneyLicenseID],
# MAGIC 			[PickupTimeID],
# MAGIC 			[DropoffTimeID],
# MAGIC 			[PickupGeographyID],
# MAGIC 			[DropoffGeographyID],
# MAGIC 			[PickupLatitude],
# MAGIC 			[PickupLongitude],
# MAGIC 			[PickupLatLong],
# MAGIC 			[DropoffLatitude],
# MAGIC 			[DropoffLongitude],
# MAGIC 			[DropoffLatLong],
# MAGIC 			[PassengerCount],
# MAGIC 			[TripDurationSeconds],
# MAGIC 			[TripDistanceMiles],
# MAGIC 			[PaymentType],
# MAGIC 			[FareAmount],
# MAGIC 			[SurchargeAmount],
# MAGIC 			[TaxAmount],
# MAGIC 			[TipAmount],
# MAGIC 			[TollsAmount],
# MAGIC 			[TotalAmount]
# MAGIC FROM [AddDWandSQLendpointToAnOtebook].[dbo].[Trip]

# METADATA ********************

# META {
# META   "language": "sql",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

import pyarrow as pa

conn = notebookutils.data.connect_to_artifact("AddDWandSQLendpointToAnOtebook", "cbfd01ac-d3b6-4081-bcbb-7920774a3fd8", "Warehouse")
sql = """
SELECT TOP (100) [DateID],
			[MedallionID],
			[HackneyLicenseID],
			[PickupTimeID],
			[DropoffTimeID],
			[PickupGeographyID],
			[DropoffGeographyID],
			[PickupLatitude],
			[PickupLongitude],
			[PickupLatLong],
			[DropoffLatitude],
			[DropoffLongitude],
			[DropoffLatLong],
			[PassengerCount],
			[TripDurationSeconds],
			[TripDistanceMiles],
			[PaymentType],
			[FareAmount],
			[SurchargeAmount],
			[TaxAmount],
			[TipAmount],
			[TollsAmount],
			[TotalAmount]
FROM [AddDWandSQLendpointToAnOtebook].[dbo].[Trip]
"""
cursor = conn.execute(sql)
columns = [column[0] for column in cursor.description]
data = cursor.fetchall()
columnar_data = list(zip(*data))
if len(columnar_data) > 0:
	arrow_table = pa.Table.from_arrays([pa.array(col) for col in columnar_data], columns)
	display(arrow_table.to_pandas())
else:
	print('empty table')


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

table_path = 'abfss://DE@onelake.dfs.fabric.microsoft.com/MSLearnDEnb.Lakehouse/Tables/dbo/fact_sale' 

import duckdb
display(duckdb.sql(f"select * from delta_scan('{table_path}') limit 1000 ").df())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# MAGIC %%tsql -artifact MSLearnDEnb -type Lakehouse -bind df3
# MAGIC SELECT TOP (10) CustomerKey
# MAGIC FROM dimension_customer

# METADATA ********************

# META {
# META   "language": "sql",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# MAGIC %%tsql -artifact AddDWandSQLendpointToAnOtebook -type Warehouse -session
# MAGIC SELECT TOP(10) * FROM AddDWandSQLendpointToAnOtebook.dbo.Trip

# METADATA ********************

# META {
# META   "language": "sql",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

df = %tsql SELECT TOP(10) * FROM AddDWandSQLendpointToAnOtebook.dbo.Trip

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

Count = 10
df = %tsql SELECT TOP ({Count}) * FROM AddDWandSQLendpointToAnOtebook.dbo.Trip

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

%tsql?

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }
