# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "717f0b50-4732-4ff0-83f4-f82d27fbe10d",
# META       "default_lakehouse_name": "deployment_logs",
# META       "default_lakehouse_workspace_id": "930d70bd-4417-46a2-bd28-105952cc6840",
# META       "known_lakehouses": [
# META         {
# META           "id": "717f0b50-4732-4ff0-83f4-f82d27fbe10d"
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

from fabric_devops import AppLogger

AppLogger.log_step('Running deployment job to create tables in gold lakehouse [sales]')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# get abfsPath path to lakehouse in same workspace with display name of 'sales_silver'
silver_lakehouse = notebookutils.lakehouse.get('sales_silver')
silver_lakehouse_path = silver_lakehouse['properties']['abfsPath']

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

AppLogger.log_step('Creating [products] table')

# create products table for gold layer

# (1) get path for silver products table
silver_products_table_path = f'{silver_lakehouse_path}/Tables/products'

# (2) load DataFrame from silver layer table
df_gold_products = (
    spark.read
         .format("delta")
         .load(silver_products_table_path)
)

# (3) write DataFrame to new gold layer table 
( df_gold_products.write
                  .mode("overwrite")
                  .option("overwriteSchema", "True")
                  .format("delta")
                  .save("Tables/products")
)

AppLogger.log_substep('table created successfully')
AppLogger.log_dataframe(df_gold_products)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

AppLogger.log_step('Creating [customers] table')

# create customers table for gold layer
from pyspark.sql.functions import concat_ws, floor, datediff, current_date, col

# (1) get path for silver customers table
silver_customers_table_path = f'{silver_lakehouse_path}/Tables/customers'

# (2) load DataFrame from silver layer table and perform transforms
df_gold_customers = (
    spark.read
         .format("delta")
         .load(silver_customers_table_path)
         .withColumnRenamed("City", "CityName")
         .withColumn("City", concat_ws(', ', col('CityName'), col('Country')) )
         .withColumn("Customer", concat_ws(' ', col('FirstName'), col('LastName')) )
         .withColumn("Age",( floor( datediff( current_date(), col("DOB") )/365.25) ))   
         .drop('FirstName', 'LastName')
)

# write DataFrame to new gold layer table 
( df_gold_customers.write
                   .mode("overwrite")
                   .option("overwriteSchema", "True")
                   .format("delta")
                   .save("Tables/customers")
)

AppLogger.log_substep('table created successfully')
AppLogger.log_dataframe(df_gold_customers)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

AppLogger.log_step('Creating [sales] table')

# create sales table for gold layer
from pyspark.sql.functions import col, desc, concat, lit, floor, datediff
from pyspark.sql.functions import date_format, to_date, current_date, year, month, dayofmonth

# (3) get paths for silver invocies and invoice_details tables
silver_invoices_table_path = f'{silver_lakehouse_path}/Tables/invoices'
silver_invoice_details_table_path = f'{silver_lakehouse_path}/Tables/invoice_details'

# load DataFrames using invoices table and invoice_details table from silver layer
df_silver_invoices = spark.read.format("delta").load(silver_invoices_table_path)
df_silver_invoice_details = spark.read.format("delta").load(silver_invoice_details_table_path)

# perform join to merge columns from both DataFrames and transform data 
df_gold_sales = (
    df_silver_invoice_details
        .join(df_silver_invoices, 
              df_silver_invoice_details['InvoiceId'] == df_silver_invoices['InvoiceId'])
        .withColumnRenamed('SalesAmount', 'Sales')
        .withColumn("DateKey", (year(col('Date'))*10000) + 
                               (month(col('Date'))*100) + 
                               (dayofmonth(col('Date')))   )
        .drop('InvoiceId', 'TotalSalesAmount', 'InvoiceId', 'Id')
        .select('Date', "DateKey", "CustomerId", "ProductId", "Sales", "Quantity")
)

# write DataFrame to new gold layer table 
( df_gold_sales.write
               .mode("overwrite")
               .option("overwriteSchema", "True")
               .format("delta")
               .save("Tables/sales")
)

AppLogger.log_substep('table created successfully')
AppLogger.log_dataframe(df_gold_sales)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

AppLogger.log_step('Creating [calander] table')

# create calendar table for gold layer
from datetime import date
import pandas as pd
from pyspark.sql.functions import to_date, year, month, dayofmonth, quarter, dayofweek, date_format

# get first and last calendar date from sakes table 
first_sales_date = df_gold_sales.agg({"Date": "min"}).collect()[0][0]
last_sales_date = df_gold_sales.agg({"Date": "max"}).collect()[0][0]

# calculate start date and end date for calendar table
start_date = date(first_sales_date.year, 1, 1)
end_date = date(last_sales_date.year, 12, 31)

# create pandas DataFrame with Date series column
df_calendar_ps = pd.date_range(start_date, end_date, freq='D').to_frame()

# convert pandas DataFrame to Spark DataFrame and add calculated calendar columns
df_calendar_spark = (
     spark.createDataFrame(df_calendar_ps)
       .withColumnRenamed("0", "timestamp")
       .withColumn("Date", to_date(col('timestamp')))
       .withColumn("DateKey", (year(col('timestamp'))*10000) + 
                              (month(col('timestamp'))*100) + 
                              (dayofmonth(col('timestamp')))   )
       .withColumn("Year", year(col('timestamp'))  )
       .withColumn("Quarter", date_format(col('timestamp'),"yyyy-QQ")  )
       .withColumn("Month", date_format(col('timestamp'),'yyyy-MM')  )
       .withColumn("Day", dayofmonth(col('timestamp'))  )
       .withColumn("MonthInYear", date_format(col('timestamp'),'MMMM')  )
       .withColumn("MonthInYearSort", month(col('timestamp'))  )
       .withColumn("DayOfWeek", date_format(col('timestamp'),'EEEE')  )
       .withColumn("DayOfWeekSort", dayofweek(col('timestamp')))
       .drop('timestamp')
)

# write DataFrame to new gold layer table 
( df_calendar_spark.write
                   .mode("overwrite")
                   .option("overwriteSchema", "True")
                   .format("delta")
                   .save("Tables/calendar")
)

AppLogger.log_substep('table created successfully')
AppLogger.log_dataframe(df_calendar_spark)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

AppLogger.log_step('Refreshing sql endpoint metadata for all tables')

spark.sql(f"REFRESH TABLE products")
spark.sql(f"REFRESH TABLE customers")
spark.sql(f"REFRESH TABLE sales")
spark.sql(f"REFRESH TABLE calendar")

AppLogger.log_substep("Refresh operations completed successfully")


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
