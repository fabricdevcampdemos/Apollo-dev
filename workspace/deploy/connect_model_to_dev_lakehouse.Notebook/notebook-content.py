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
