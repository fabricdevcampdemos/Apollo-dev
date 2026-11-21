# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "4a8dc83b-3095-4037-a157-0201d77672fa",
# META       "default_lakehouse_name": "deployment_logs",
# META       "default_lakehouse_workspace_id": "000b9dfd-7a15-4fca-864a-46a3ca09a85a",
# META       "known_lakehouses": [
# META         {
# META           "id": "4a8dc83b-3095-4037-a157-0201d77672fa"
# META         }
# META       ]
# META     },
# META     "environment": {
# META       "environmentId": "254abdfb-8d60-48f2-b425-23b1fc484efb",
# META       "workspaceId": "c8fa8028-9944-413e-9c28-426a7cbf192a"
# META     }
# META   }
# META }

# PARAMETERS CELL ********************

lakehouse_name = 'deployment_logs'

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from fabric_devops import AppLogger, FabricRestApi
	
lakehouse = FabricRestApi.get_item_by_name(lakehouse_name, 'Lakehouse')
FabricRestApi.wait_for_lakehouse_provisioning_to_complete(lakehouse)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
