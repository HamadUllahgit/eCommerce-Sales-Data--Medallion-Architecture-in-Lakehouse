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

# ###### **<mark>Creating Gold_Product table</mark>**

# CELL ********************

# MAGIC %%sql
# MAGIC create table if not exists Ecommerce_project.gold_product
# MAGIC (
# MAGIC     Product_ID int,
# MAGIC     Product_Category string,
# MAGIC     Product string,
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

# ###### **<mark>Building logic for incremental load</mark>**

# CELL ********************

Max_Date=spark.sql("select coalesce(max(Modified_TS),'1900-01-01') from Ecommerce_project.gold_product").first()[0]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ###### **<mark>Creating dataframe with required columns for gold Custimer table</mark>**

# CELL ********************

df_bronze=spark.sql("select distinct Product_Category, Product from Ecommerce_project.bronze_sales where Modified_TS>'{}'".format(Max_Date))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ###### **<mark>Creating Product_ID column using montonically_increasing_id</mark>**

# CELL ********************

Max_ID = spark.sql("select coalesce(max(Product_ID),0) from Ecommerce_project.gold_product").first()[0]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

final_df= df_bronze.withColumn('Product_ID',monotonically_increasing_id()+Max_ID+1)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ###### **<mark>Creating view from final_df</mark>**

# CELL ********************

final_df.createOrReplaceTempView("ViewProduct")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ###### **<mark>Inserting data from View to gold_product using merge query</mark>**

# CELL ********************

# MAGIC %%sql
# MAGIC MERGE INTO Ecommerce_project.gold_Product as gp 
# MAGIC USING ViewProduct as vp
# MAGIC on gp.Product = vp.Product and gp.Product_Category=vp.Product_Category
# MAGIC WHEN MATCHED THEN
# MAGIC 
# MAGIC UPDATE SET
# MAGIC gp.Modified_TS = CURRENT_TIMESTAMP()
# MAGIC 
# MAGIC WHEN NOT MATCHED THEN
# MAGIC INSERT
# MAGIC (
# MAGIC gp.Product_ID,
# MAGIC gp.Product,
# MAGIC gp.Product_Category,
# MAGIC gp.Created_TS,
# MAGIC gp.Modified_TS
# MAGIC 
# MAGIC )
# MAGIC VALUES
# MAGIC (
# MAGIC vp.Product_ID,
# MAGIC vp.Product,
# MAGIC vp.Product_Category,
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
