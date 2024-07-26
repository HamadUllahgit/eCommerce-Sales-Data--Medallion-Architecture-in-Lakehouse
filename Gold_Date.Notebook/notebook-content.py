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

# ###### **<mark>Generating date from rang and capturing it in dataframe</mark>**

# CELL ********************

# Generate date range from 2015-01-01 to 2030-12-31
date_df = spark.range(0, 5844) \
    .select(expr("date_add(to_date('2015-01-01'), cast(id as int)) as Business_Date"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ###### <mark>**Extracting required columns from data**</mark>

# CELL ********************


date_df = date_df \
    .withColumn("Year", year(col("Business_Date")).cast("int")) \
    .withColumn("Quarter", quarter(col("Business_Date")).cast("int")) \
    .withColumn("Month", month(col("Business_Date")).cast("int")) \
    .withColumn("Week", weekofyear(col("Business_Date")).cast("int"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ###### **<mark>Filtering date to captured the desired range </mark>**

# CELL ********************

# Filter to range from 2015 to 2030
date_df = date_df.filter((col("Year") >= 2015) & (col("Year") <= 2030))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ###### **<mark>Writing data from date_df to Date Table</mark>**

# CELL ********************

delta_table_name = 'Gold_Date'
date_df.write.mode("Overwrite").format('delta').saveAsTable(delta_table_name)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
