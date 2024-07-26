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
from notebookutils import mssparkutils

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ###### <mark>**Now we will mount to our files folder**</mark>

# CELL ********************

mssparkutils.fs.mount("abfss://3e1f0f4e-4072-4fbe-86fe-4c969e139d91@onelake.dfs.fabric.microsoft.com/b2821490-f415-4bb0-846e-6cf5cc2fde87/Files","/Files")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ###### **<mark>Checking the mount path</mark>**

# CELL ********************

mssparkutils.fs.getMountPath('/Files')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ###### **<mark>Creating Check_file variable to check if files exist in our current folder or not</mark>**

# CELL ********************

check_files =mssparkutils.fs.ls(f"file://{mssparkutils.fs.getMountPath('/Files')}/current")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ###### **<mark>Building load logic, if check_files is not null then we will run all the notebooks</mark>**

# CELL ********************

if check_files:
    mssparkutils.notebook.run("Bronze_sales")
    mssparkutils.notebook.run("Gold_Customer")
    mssparkutils.notebook.run("Gold_Date")
    mssparkutils.notebook.run("Gold_fact_Sale")
    mssparkutils.notebook.run("Gold_OrderPriority")
    mssparkutils.notebook.run("Gold_OrderReturn")
    mssparkutils.notebook.run("Gold_Product")
    mssparkutils.notebook.run("Gold_shipmode")
    mssparkutils.notebook.run("Archive_Files")

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
