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

# ###### **<mark>This code provide the functionality of mssparkutils api</mark>**

# CELL ********************

mssparkutils.fs.help()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ###### **<mark>Mounting to files folder</mark>**

# CELL ********************

mssparkutils.fs.mount("abfss://3e1f0f4e-4072-4fbe-86fe-4c969e139d91@onelake.dfs.fabric.microsoft.com/b2821490-f415-4bb0-846e-6cf5cc2fde87/Files","/Files")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ###### **<mark>Creating mount path</mark>**

# CELL ********************

mssparkutils.fs.getMountPath('/Files')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ###### <mark>**Creating check_files variable to check if files exists in current folder**</mark>

# CELL ********************

check_files=mssparkutils.fs.ls(f"file://{mssparkutils.fs.getMountPath('/Files')}/current/")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ###### **<mark>Building logic, if files exist in current folder then move it to archive</mark>**

# CELL ********************

for file in check_files:
    if file.path:
        mssparkutils.fs.mv(file.path,f"file://{mssparkutils.fs.getMountPath('/Files')}/Archive/")

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
