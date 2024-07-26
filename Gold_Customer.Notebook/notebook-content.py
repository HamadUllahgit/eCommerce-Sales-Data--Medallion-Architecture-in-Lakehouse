# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "b2821490-f415-4bb0-846e-6cf5cc2fde87",
# META       "default_lakehouse_name": "Ecommerce_project",
# META       "default_lakehouse_workspace_id": "3e1f0f4e-4072-4fbe-86fe-4c969e139d91",
# META       "known_lakehouses": [
# META         {
# META           "id": "b2821490-f415-4bb0-846e-6cf5cc2fde87"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import *

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ###### **<mark>Reading data from bronze sales into dataframe</mark>**

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE TABLE if not EXISTS Ecommerce_project.gold_customer
# MAGIC (
# MAGIC Customer_ID string,
# MAGIC Customer_Name string,
# MAGIC Segment string,
# MAGIC City string,
# MAGIC State string,
# MAGIC Country string,
# MAGIC Region string,
# MAGIC Created_TS timestamp,
# MAGIC Modified_TS timestamp
# MAGIC )
# MAGIC USING DELTA

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

Max_Date=spark.sql("select coalesce(max(Modified_TS),'1900-01-01') from Ecommerce_project.gold_customer").first()[0]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ###### **<mark>Creating dataframe with required columns for gold Custimer table</mark>**

# CELL ********************

df_bronze = spark.read.table("Ecommerce_project.bronze_sales")
df_customer = df_bronze.selectExpr("Customer_ID","Customer_Name","Segment",\
                        "City","State","Country","Region")\
                        .where(col("Modified_TS")>Max_Date) \
                        .dropDuplicates()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ###### **<mark>Inserting data from Customer_df to dimension table i.e. Gold_customer</mark>**

# CELL ********************

df_customer.createOrReplaceTempView('ViewCustomer')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC MERGE INTO Ecommerce_project.gold_customer as gc
# MAGIC USING ViewCustomer as vc
# MAGIC on gc.Customer_ID = vc.Customer_ID
# MAGIC 
# MAGIC WHEN MATCHED THEN
# MAGIC UPDATE set 
# MAGIC gc.Customer_Name = vc.Customer_Name,
# MAGIC gc.Segment = vc.Segment,
# MAGIC gc.City = vc.City,
# MAGIC gc.State = vc.State,
# MAGIC gc.Country = vc.Country,
# MAGIC gc.Region = vc.Region,
# MAGIC gc.Modified_TS = CURRENT_TIMESTAMP
# MAGIC 
# MAGIC WHEN NOT MATCHED THEN
# MAGIC INSERT
# MAGIC (
# MAGIC gc.Customer_ID,
# MAGIC gc.Customer_Name,
# MAGIC gc.Segment,
# MAGIC gc.City ,
# MAGIC gc.State,
# MAGIC gc.Country,
# MAGIC gc.Region ,
# MAGIC gc.Created_TS,
# MAGIC gc.Modified_TS
# MAGIC )
# MAGIC VALUES
# MAGIC (
# MAGIC vc.Customer_ID,
# MAGIC vc.Customer_Name,
# MAGIC vc.Segment,
# MAGIC vc.City ,
# MAGIC vc.State,
# MAGIC vc.Country,
# MAGIC vc.Region ,
# MAGIC CURRENT_TIMESTAMP(),
# MAGIC CURRENT_TIMESTAMP()
# MAGIC )

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
