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

# ###### <mark>**Creating gold_fact_sale**</mark>

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE TABLE if not exists Ecommerce_project.gold_fact_sale
# MAGIC (
# MAGIC     Order_ID string,
# MAGIC     Price float,
# MAGIC     Quantity float,
# MAGIC     Sales float,
# MAGIC     Discount float,
# MAGIC     Profit float,
# MAGIC     Shipping_Cost float,
# MAGIC     Order_Date date,
# MAGIC     Shipping_Date date,
# MAGIC     Product_ID integer,
# MAGIC     Priority_ID integer,
# MAGIC     ShipMode_ID integer,
# MAGIC     Customer_ID string,
# MAGIC     Order_Year integer,
# MAGIC     Order_Month integer,
# MAGIC     Created_TS timestamp,
# MAGIC     Modified_TS timestamp
# MAGIC )
# MAGIC USING DELTA
# MAGIC PARTITIONED BY (Order_Year,Order_Month)

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ###### **<mark>Creating data frame with required columns</mark>**

# CELL ********************

bronze_df = spark.sql("""select
bronze_sales.Order_ID,
bronze_sales.Sales as Price,
bronze_sales.Quantity,
bronze_sales.Sales * bronze_sales.Quantity as Sales,
bronze_sales.Discount,
bronze_sales.Profit,
bronze_sales.Shipping_Cost,
bronze_sales.Order_Date,
bronze_sales.Shipping_Date,
gold_product.Product_ID,
gold_orderpriority.Priority_ID,
gold_shipmode.ShipMode_ID,
bronze_sales.Customer_ID,
Year(Order_Date) as Order_Year,
Month(Order_Date) as Order_Month
from Ecommerce_project.bronze_sales
INNER JOIN Ecommerce_project.gold_product on bronze_sales.Product=gold_product.Product and
                                             bronze_sales.Product_Category=gold_product.Product_Category
INNER JOIN Ecommerce_project.gold_orderpriority on bronze_sales.Order_Priority=gold_orderpriority.Order_Priority
INNER JOIN Ecommerce_project.gold_shipmode on bronze_sales.Ship_Mode=gold_shipmode.Ship_Mode
""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ###### **<mark>Creating view from data frame</mark>**

# CELL ********************

bronze_df.createOrReplaceTempView("ViewfactSale")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ###### **<mark>Inserting data to gold_fact_sale from view using merge query</mark>**

# CELL ********************

# MAGIC %%sql
# MAGIC merge INTO Ecommerce_project.gold_fact_sale as gfs
# MAGIC USING ViewfactSale as vfs
# MAGIC on gfs.Order_Year=vfs.Order_Year and gfs.Order_Month=vfs.Order_Month and gfs.Order_ID=vfs.Order_ID
# MAGIC WHEN MATCHED THEN
# MAGIC UPDATE SET
# MAGIC gfs.Sales = vfs.Sales,
# MAGIC gfs.Price = vfs.Price,
# MAGIC gfs.Discount =vfs.Discount,
# MAGIC gfs.Quantity =vfs.Quantity,
# MAGIC gfs.Profit = vfs.Profit,
# MAGIC gfs.Shipping_Cost =vfs.Shipping_Cost,
# MAGIC gfs.Order_Date =vfs.Order_Date,
# MAGIC gfs.Shipping_Date =vfs.Shipping_Date,
# MAGIC gfs.Product_ID =vfs.Product_ID,
# MAGIC gfs.Priority_ID =vfs.Priority_ID,
# MAGIC gfs.ShipMode_ID =vfs.ShipMode_ID,
# MAGIC gfs.Customer_ID =vfs.Customer_ID,
# MAGIC gfs.Modified_TS =current_timestamp()
# MAGIC 
# MAGIC 
# MAGIC WHEN NOT MATCHED THEN
# MAGIC INSERT
# MAGIC (
# MAGIC gfs.Order_ID,
# MAGIC gfs.Price ,
# MAGIC gfs.Quantity ,
# MAGIC gfs.Sales,
# MAGIC gfs.Discount ,
# MAGIC gfs.Profit ,
# MAGIC gfs.Shipping_Cost,
# MAGIC gfs.Order_Date ,
# MAGIC gfs.Shipping_Date ,
# MAGIC gfs.Product_ID,
# MAGIC gfs.Priority_ID,
# MAGIC gfs.ShipMode_ID,
# MAGIC gfs.Customer_ID,
# MAGIC gfs.Order_Year,
# MAGIC gfs.Order_Month,
# MAGIC gfs.Created_TS,
# MAGIC gfs.Modified_TS
# MAGIC )
# MAGIC VALUES
# MAGIC (
# MAGIC vfs.Order_ID,
# MAGIC vfs.Price ,
# MAGIC vfs.Quantity ,
# MAGIC vfs.Sales,
# MAGIC vfs.Discount ,
# MAGIC vfs.Profit ,
# MAGIC vfs.Shipping_Cost,
# MAGIC vfs.Order_Date ,
# MAGIC vfs.Shipping_Date ,
# MAGIC vfs.Product_ID,
# MAGIC vfs.Priority_ID,
# MAGIC vfs.ShipMode_ID,
# MAGIC vfs.Customer_ID,
# MAGIC vfs.Order_Year,
# MAGIC vfs.Order_Month,
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
