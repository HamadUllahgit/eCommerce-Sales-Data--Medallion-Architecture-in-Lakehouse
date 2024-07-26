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

# ###### **<mark>Creating dimension table of Order_Priority</mark>**

# CELL ********************

# MAGIC %%sql
# MAGIC create table if not exists Ecommerce_project.Gold_OrderPriority
# MAGIC (
# MAGIC     Order_Priority string,
# MAGIC     Priority_ID int,
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

# ###### **<mark>Building increamental load</mark>**

# CELL ********************

Max_Date=spark.sql("select coalesce(max(Modified_TS),'1900-01-01') from Ecommerce_project.gold_orderpriority").first()[0]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ###### **<mark>Creating dataframe with required columns for gold Order_priority table</mark>**

# CELL ********************

df_bronze=spark.sql("select distinct Order_Priority from Ecommerce_project.bronze_sales where Modified_TS>'{}'".format(Max_Date))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ###### **<mark>Creating priority_ID columns using montinically_increasing_id</mark>**

# CELL ********************

Max_ID = spark.sql("select coalesce(max(Priority_ID),0) from Ecommerce_project.gold_orderpriority").first()[0]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

final_df= df_bronze.withColumn('Priority_ID',monotonically_increasing_id()+Max_ID+1)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ###### **<mark>Creating temporary view from df we just created</mark>**

# CELL ********************

final_df.createOrReplaceTempView("ViewOrderPriority")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ###### **<mark>Inserting data from view to gold_table</mark>**

# CELL ********************

# MAGIC %%sql
# MAGIC MERGE INTO Ecommerce_project.gold_orderpriority as gop
# MAGIC USING ViewOrderPriority as vop
# MAGIC on gop.Order_Priority = vop.Order_Priority
# MAGIC WHEN MATCHED THEN
# MAGIC 
# MAGIC UPDATE SET
# MAGIC gop.Modified_TS = CURRENT_TIMESTAMP()
# MAGIC 
# MAGIC WHEN NOT MATCHED THEN
# MAGIC INSERT
# MAGIC (
# MAGIC gop.Priority_ID,
# MAGIC gop.Order_Priority,
# MAGIC gop.Created_TS,
# MAGIC gop.Modified_TS
# MAGIC 
# MAGIC )
# MAGIC VALUES
# MAGIC (
# MAGIC vop.Priority_ID,
# MAGIC vop.Order_Priority,
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
