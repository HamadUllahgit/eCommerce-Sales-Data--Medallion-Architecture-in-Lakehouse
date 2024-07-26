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

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ###### **<mark>Creating Gold order_return Table</mark>**

# CELL ********************

# MAGIC %%sql
# MAGIC create table if not exists Ecommerce_project.Gold_OrderReturn
# MAGIC (
# MAGIC     Order_ID string,
# MAGIC     Return string,
# MAGIC     Order_year int,
# MAGIC     Order_Month int,
# MAGIC     Created_TS TIMESTAMP,
# MAGIC     Modified_TS TIMESTAMP
# MAGIC )
# MAGIC USING DELTA
# MAGIC PARTITIONED by (Order_year,Order_Month)

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ###### <mark>**Designing Incremental load for Gold_OrderReturn Table**</mark>

# CELL ********************

Max_Date=spark.sql("SELECT coalesce(max('Modified_TS'),'1900-01-01') from Ecommerce_project.Gold_orderReturn").first()[0]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ###### <mark>**Adding columns to data frame**</mark>

# CELL ********************

df=spark.sql(
"""SELECT Order_ID,
Return,
Order_year,
Order_Month,
Created_TS,
Modified_TS
FROM Ecommerce_project.bronze_sales where Return="Yes" and Modified_TS >'{}'""".format(Max_Date))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ###### **<mark>Storing this dataframe in temporary view</mark>**

# CELL ********************

df.createOrReplaceTempView("ViewReturn")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ###### **<mark>Inserting the data from view to Gold_OrderReturn Table</mark>**

# CELL ********************

# MAGIC %%sql
# MAGIC INSERT INTO Ecommerce_project.Gold_OrderReturn
# MAGIC SELECT * FROM ViewReturn

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
