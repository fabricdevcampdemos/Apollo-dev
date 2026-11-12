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

# CELL ********************

from fabric_devops import AppLogger, FabricRestApi, PowerBiRestApi, ItemDefinitionFactory

AppLogger.log_step('Running deployment job to connect semantic model to lakehouse in dev workspace')

try:

    model = FabricRestApi.get_item_by_name('Product Sales DirectLake Model', 'SemanticModel')

    sql_datasource = PowerBiRestApi.get_sql_dataource_from_semantic_model(model['id'])

    connection = FabricRestApi.create_sql_connection_with_service_principal(
        sql_datasource['server'],
        sql_datasource['database'])

    AppLogger.log_substep('Binding semantic model to ADLS connection')
    PowerBiRestApi.bind_semantic_model_to_connection(model['id'], connection['id'])

    AppLogger.log_substep('Refreshing imported data for semantic model')
    PowerBiRestApi.refresh_semantic_model(model['id'])

except Exception as e:
    AppLogger.log_step(f'ERROR: {type(e).__name__} - {e}')

    


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
