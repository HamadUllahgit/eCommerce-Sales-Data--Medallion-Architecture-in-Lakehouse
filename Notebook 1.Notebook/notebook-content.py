# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "31368fed-bd91-4cb5-b8f9-b63e11c80179",
# META       "default_lakehouse_name": "New_lakhouse",
# META       "default_lakehouse_workspace_id": "3e1f0f4e-4072-4fbe-86fe-4c969e139d91",
# META       "known_lakehouses": [
# META         {
# META           "id": "33abb06e-6ede-4bfa-affd-4f32ecea25b4"
# META         },
# META         {
# META           "id": "31368fed-bd91-4cb5-b8f9-b63e11c80179"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!
## Loading the data to Spark 

csv_path= "abfss://3e1f0f4e-4072-4fbe-86fe-4c969e139d91@onelake.dfs.fabric.microsoft.com/33abb06e-6ede-4bfa-affd-4f32ecea25b4/Files/Sales by Store.csv"

sale_df = spark.read.csv(csv_path, header=True,inferSchema=True)

display(sale_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # **<mark>Creating sub_folder to write the data as parquet. </mark>**

# CELL ********************

sale_df.write.json("Files/Formats/JSON",mode='overwrite')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

New_df= spark.read.parquet('abfss://3e1f0f4e-4072-4fbe-86fe-4c969e139d91@onelake.dfs.fabric.microsoft.com/33abb06e-6ede-4bfa-affd-4f32ecea25b4/Files/Parquet', header= True) 
display(New_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

sale_df.write.parquet("Files/Formats/Parquet",mode='overwrite')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## <mark>**Reading multiple files from a folder**</mark>

# CELL ********************

# Reading all the parquet files from Parquet folder
sales_parquet = spark.read.parquet('Files/Formats/*Parquet', header= True)
display(sales_parquet)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Reading all parquet files then adding metadata 

sales_parquet_metadata = spark.read.parquet('Files/Formats/*Parquet', header = True).select('*', '_metadata')

display(sales_parquet_metadata)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(sales_parquet_metadata)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

sale_df.dtypes

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### <mark>### **In the below code we are going to change the names of the columns **</mark>

# CELL ********************

sale_df = sale_df.withColumnRenamed("transaction_id", "TransactionID").withColumnRenamed("customer_id", "CustomerID")
display(sale_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### **<mark>Writing Data Frame to delta tables</mark>**

# CELL ********************

delta_table_name = 'Sale'

sale_df.write.mode('overwrite').format('delta').saveAsTable(delta_table_name)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### **<mark>Filtering</mark>**

# CELL ********************

#Filtering data frame where store_id is equal to 5. (For not equal to we use !=)
sale_df.filter(sale_df['store_id']==5).show()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#changing data type of transaction date from data&time to time only
from pyspark.sql.functions import col, date_format

sale_df = sale_df.withColumn('transaction_time',date_format(col('transaction_time'),'HH:mm:ss'))
display(sale_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### **<mark>Multiple filtering</mark>**

# CELL ********************

## Filtering dataframe where store ID = 5 and price is greater than 3 (Instead of & we can use OR functionality as well for OR we can use '|' this)

sale_df.filter((sale_df.store_id == 5) & (sale_df.unit_price > 3)).show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

delta_table_name = 'New_sale'

sale_df.write.mode('overwrite').format('delta').saveAsTable(delta_table_name)

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
