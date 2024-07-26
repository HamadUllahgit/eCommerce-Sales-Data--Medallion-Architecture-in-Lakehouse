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
# Type here in the cell editor to add code
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.functions import *
import pandas as pd

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ###### **<mark>Reading Files from lakehouse using pandas</mark>**

# CELL ********************

df_sales = pd.read_excel('abfss://3e1f0f4e-4072-4fbe-86fe-4c969e139d91@onelake.dfs.fabric.microsoft.com/b2821490-f415-4bb0-846e-6cf5cc2fde87/Files/current/Sales*.xlsx',sheet_name='Sales')
df_return = pd.read_excel('abfss://3e1f0f4e-4072-4fbe-86fe-4c969e139d91@onelake.dfs.fabric.microsoft.com/b2821490-f415-4bb0-846e-6cf5cc2fde87/Files/current/Sales*.xlsx',sheet_name='Returns')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ###### <mark>**Passing the pandas dataframe to spark**</mark>

# CELL ********************

df1 = spark.createDataFrame(df_sales)
df2 = spark.createDataFrame(df_return)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ###### **<mark>Checking both first 5 rows of both datafram</mark>**

# CELL ********************

display(df1.head(5))
display(df2.head(5))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ###### <mark>**Joing both dataframes using Order_ID column**</mark>

# MARKDOWN ********************

# ###### <mark>**Since we needed only Return columns and the rest are being repeated so we are going to drop it**</mark>

# CELL ********************

final_df = df1.join(df2,df1.Order_ID==df2.Order_ID,how='left').drop(df2.Order_ID,df2.Customer_Name, df2.Sales_Amount)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ###### <mark>**Adding Month, Year, Created_TS and Modified_TS columns to dataframe**</mark>

# CELL ********************

final_df = final_df.withColumns({"Order_year":year("Order_Date"),\
                    "Order_Month":month("Order_Date"),\
                    "Created_TS": current_timestamp(),\
                    "Modified_TS":current_timestamp(),\
                    })

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ###### <mark>**Creating View from final dataframe**</mark>

# CELL ********************

final_df.createOrReplaceTempView("ViewSales")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ###### **<mark>Creating Bronze_sales table in our lakehouse</mark>**

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE TABLE if NOT EXISTS bronze_sales
# MAGIC (
# MAGIC   Order_ID	string,
# MAGIC     Order_Date	Date,
# MAGIC     Shipping_Date	date,
# MAGIC     Aging	int,
# MAGIC     Ship_Mode	string,
# MAGIC     Product_Category	string,
# MAGIC     Product	 string,
# MAGIC     Sales 	float,
# MAGIC     Quantity	float,
# MAGIC     Discount	 float,
# MAGIC     Profit 	 float,
# MAGIC     Shipping_Cost 	float,
# MAGIC     Order_Priority	string,
# MAGIC     Customer_ID	string,
# MAGIC     Customer_Name	string,
# MAGIC     Segment	string,
# MAGIC     City	string,
# MAGIC     State	string,
# MAGIC     Country	string,
# MAGIC     Region string,
# MAGIC 	Return string,
# MAGIC     Order_Year int,
# MAGIC     Order_Month int,
# MAGIC     Created_TS  TIMESTAMP,
# MAGIC     Modified_TS  TIMESTAMP
# MAGIC )
# MAGIC USING DELTA
# MAGIC PARTITIONED BY (Order_Year,Order_Month)

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ###### **<mark>Inserting data from ViewSales to bronze_sales table using Merge query</mark>**

# CELL ********************

# MAGIC %%sql
# MAGIC Merge into  Ecommerce_project.bronze_sales as BS
# MAGIC using ViewSales as VS
# MAGIC on  BS.Order_Year=VS.Order_Year and BS.Order_Month=VS.Order_Month and BS.Order_ID=VS.Order_ID
# MAGIC when matched then 
# MAGIC update SET
# MAGIC BS.Order_Date	=	VS.Order_Date	,
# MAGIC BS.Shipping_Date	=	VS.Shipping_Date	,
# MAGIC BS.Aging	=	VS.Aging	,
# MAGIC BS.Ship_Mode	=	VS.Ship_Mode	,
# MAGIC BS.Product_Category	=	VS.Product_Category	,
# MAGIC BS.Product	=	VS.Product	,
# MAGIC BS.Sales 	=	VS.Sales 	,
# MAGIC BS.Quantity	=	VS.Quantity	,
# MAGIC BS.Discount	=	VS.Discount	,
# MAGIC BS.Profit 	=	VS.Profit 	,
# MAGIC BS.Shipping_Cost 	=	VS.Shipping_Cost 	,
# MAGIC BS.Order_Priority	=	VS.Order_Priority	,
# MAGIC BS.Customer_ID	=	VS.Customer_ID	,
# MAGIC BS.Customer_Name	=	VS.Customer_Name	,
# MAGIC BS.Segment	=	VS.Segment	,
# MAGIC BS.City	=	VS.City	,
# MAGIC BS.State	=	VS.State	,
# MAGIC BS.Country	=	VS.Country	,
# MAGIC BS.Region  	=	VS.Region  	,
# MAGIC BS.Return  	=	VS.Return  	,
# MAGIC BS.Modified_TS	=	VS.Modified_TS	
# MAGIC 
# MAGIC when not matched then 
# MAGIC INSERT
# MAGIC (
# MAGIC BS.Order_ID,    
# MAGIC BS.Order_Date	,
# MAGIC BS.Shipping_Date	,
# MAGIC BS.Aging	,
# MAGIC BS.Ship_Mode	,
# MAGIC BS.Product_Category	,
# MAGIC BS.Product	,
# MAGIC BS.Sales 	,
# MAGIC BS.Quantity	,
# MAGIC BS.Discount	,
# MAGIC BS.Profit 	,
# MAGIC BS.Shipping_Cost 	,
# MAGIC BS.Order_Priority	,
# MAGIC BS.Customer_ID	,
# MAGIC BS.Customer_Name	,
# MAGIC BS.Segment	,
# MAGIC BS.City	,
# MAGIC BS.State	,
# MAGIC BS.Country	,
# MAGIC BS.Region  	,
# MAGIC BS.Return,
# MAGIC BS.Order_Year  	,
# MAGIC BS.Order_Month  	,
# MAGIC BS.Created_TS   	,
# MAGIC BS.Modified_TS	
# MAGIC )
# MAGIC values
# MAGIC (
# MAGIC VS.Order_ID,
# MAGIC VS.Order_Date	,
# MAGIC VS.Shipping_Date	,
# MAGIC VS.Aging	,
# MAGIC VS.Ship_Mode	,
# MAGIC VS.Product_Category	,
# MAGIC VS.Product	,
# MAGIC VS.Sales 	,
# MAGIC VS.Quantity	,
# MAGIC VS.Discount	,
# MAGIC VS.Profit 	,
# MAGIC VS.Shipping_Cost 	,
# MAGIC VS.Order_Priority	,
# MAGIC VS.Customer_ID	,
# MAGIC VS.Customer_Name	,
# MAGIC VS.Segment	,
# MAGIC VS.City	,
# MAGIC VS.State	,
# MAGIC VS.Country	,
# MAGIC VS.Region  	,
# MAGIC VS.Return,
# MAGIC VS.Order_Year  	,
# MAGIC VS.Order_Month  	,
# MAGIC VS.Created_TS   	,
# MAGIC VS.Modified_TS	
# MAGIC 
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
