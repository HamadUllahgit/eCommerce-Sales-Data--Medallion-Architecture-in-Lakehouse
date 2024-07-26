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
from pyspark.sql.functions import col, date_format, year, quarter, month, weekofyear

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ###### **<mark>Creating dimension table of Shipmode</mark>**

# CELL ********************

# MAGIC %%sql
# MAGIC create table if not exists Ecommerce_project.Gold_Shipmode
# MAGIC (
# MAGIC     Ship_Mode string,
# MAGIC     ShipMode_ID int,
# MAGIC     Created_TS TIMESTAMP,
# MAGIC     Modified_TS TIMESTAMP
# MAGIC )
# MAGIC USING DELTA 

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ###### **<mark>Build incremental load</mark>**

# CELL ********************

Max_Date=spark.sql("select coalesce(max(Modified_TS),'1900-01-01') from Ecommerce_project.gold_shipmode").first()[0]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ###### **<mark>Creating dataframe with required columns for gold Ship_Mode table</mark>**

# CELL ********************

df_bronze=spark.sql("select distinct Ship_Mode from Ecommerce_project.bronze_sales where Modified_TS>'{}'".format(Max_Date))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ###### **<mark>Creating logic for ShipMode_ID column</mark>**

# CELL ********************

Max_ID = spark.sql("select coalesce(max(ShipMode_ID),0) from Ecommerce_project.gold_shipmode").first()[0]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ###### <mark>**Adding ShipMode_ID to dataframe using monotonically_increasing_id**</mark>

# CELL ********************

final_df= df_bronze.withColumn('ShipMode_ID',monotonically_increasing_id()+Max_ID+1)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ###### **<mark>Creating view from final_df</mark>**

# CELL ********************

final_df.createOrReplaceTempView('ViewShipMode')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ###### <mark>**Inserting data from view to Gold_ShipMode Table using Merge Query**</mark>

# CELL ********************

# MAGIC %%sql
# MAGIC MERGE INTO Ecommerce_project.gold_shipmode as gs
# MAGIC USING ViewShipMode as vs 
# MAGIC on gs.Ship_Mode = vs.Ship_Mode
# MAGIC 
# MAGIC WHEN MATCHED THEN
# MAGIC UPDATE SET
# MAGIC Modified_TS = CURRENT_TIMESTAMP()
# MAGIC 
# MAGIC WHEN NOT MATCHED THEN
# MAGIC INSERT
# MAGIC (
# MAGIC gs.Ship_Mode,
# MAGIC gs.ShipMode_ID,
# MAGIC gs.Created_TS,
# MAGIC gs.Modified_TS
# MAGIC )
# MAGIC VALUES
# MAGIC (
# MAGIC vs.Ship_Mode,
# MAGIC vs.ShipMode_ID,
# MAGIC CURRENT_TIMESTAMP(),
# MAGIC CURRENT_TIMESTAMP
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
