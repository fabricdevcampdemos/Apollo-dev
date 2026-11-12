# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "b208ab94-02a8-4a56-8389-11d711ab3abf",
# META       "default_lakehouse_name": "deployment_logs",
# META       "default_lakehouse_workspace_id": "930d70bd-4417-46a2-bd28-105952cc6840",
# META       "known_lakehouses": [
# META         {
# META           "id": "b208ab94-02a8-4a56-8389-11d711ab3abf"
# META         }
# META       ]
# META     },
# META     "environment": {
# META       "environmentId": "52a2123f-0c5e-4325-85b7-9daebbb36882",
# META       "workspaceId": "d3b0e199-c9e9-455e-880d-935545441dcf"
# META     }
# META   }
# META }

# PARAMETERS CELL ********************

target_environment = None

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from fabric_devops import AppLogger, FabricRestApi, PowerBiRestApi

if target_environment in ['dev', 'test', 'prod']:
    variable_library = FabricRestApi.get_item_by_name('environment_settings', "VariableLibrary")
    FabricRestApi.set_active_valueset_for_variable_library(variable_library, target_environment)
else:
    AppLogger.log_step(f'Cannot set active value set to [{target_environment}]')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

AppLogger.end_logging_session()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
