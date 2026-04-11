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

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!
notebookutils.fs.help()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

notebookutils.fs.ls('abfss://DE@onelake.dfs.fabric.microsoft.com/MSLearnDEnb.Lakehouse/Tables/dbo/dimension_city')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

notebookutils.fs.mkdirs('abfss://DE@onelake.dfs.fabric.microsoft.com/MSLearnDEnb.Lakehouse/Files/ToNBU')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

notebookutils.fs.cp('abfss://DE@onelake.dfs.fabric.microsoft.com/MSLearnDEnb.Lakehouse/Files/FromNBU/','abfss://DE@onelake.dfs.fabric.microsoft.com/MSLearnDEnb.Lakehouse/Files/ToNBU/',recurse=True)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

notebookutils.fs.rm('abfss://DE@onelake.dfs.fabric.microsoft.com/MSLearnDEnb.Lakehouse/Files/ToNBU', recurse=True)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

files = notebookutils.fs.ls('abfss://DE@onelake.dfs.fabric.microsoft.com/MSLearnDEnb.Lakehouse/Files/FromNBU')
for file in files:
    print(file.name, file.isDir, file.isFile, file.path, file.size)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

files = notebookutils.fs.ls("Files/")
for file in files:
    print(f"Name: {file.name}, Size: {file.size}, IsDir: {file.isDir}, Path: {file.path}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

content = notebookutils.fs.head('abfss://DE@onelake.dfs.fabric.microsoft.com/MSLearnDEnb.Lakehouse/Files/FromNBU/data.csv',1000)
print(content)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.read.format("csv").option("header","true").load("Files/FromNBU/data.csv")
# df now is a Spark DataFrame containing CSV data from "Files/FromNBU/data.csv".
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

notebookutils.fs.mount( 
 "abfss://DE@onelake.dfs.fabric.microsoft.com/MSLearnDEnb.Lakehouse", 
 "/mountname"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

notebookutils.fs.mounts()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
