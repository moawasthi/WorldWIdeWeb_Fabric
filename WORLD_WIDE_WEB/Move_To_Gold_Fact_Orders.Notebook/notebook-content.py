# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "a2ade130-09f7-4383-94bc-87d55b950f45",
# META       "default_lakehouse_name": "SilverLayer",
# META       "default_lakehouse_workspace_id": "f70b78b0-968f-4c16-82df-35b50fbb2937",
# META       "known_lakehouses": [
# META         {
# META           "id": "a2ade130-09f7-4383-94bc-87d55b950f45"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

from pyspark.sql.functions import col, datediff

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_orders = spark.read.table('orders_oltp')
df_orderLine = spark.read.table('orderline_oltp')
df_date = spark.table('date')
df_customer = spark.read.table('GoldLayer.DimCustomer')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_orders = df_orders.select('OrderID','OrderDate', 'PickingCompletedWhen', 'CustomerID')
df_orderLine = df_orderLine.select('OrderID', 'Description', 'Quantity', 'UnitPrice', 'TaxRate', 'PickedQuantity')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

selectedColumns = ["OrderDate", "PickingCompletedWhen", "CustomerID", "Description", "Quantity", "UnitPrice", "TaxRate" ,"PickedQuantity", "o.OrderID"]

df_fact_orders = df_orders.alias("o").join(df_orderLine.alias("ol"), on = col('o.OrderID') == col('ol.OrderID') , how="left" ).select(selectedColumns) 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

selectedColumns = ["OrderDate", "PickingCompletedWhen", "c.Customer_Key", "Description", "Quantity", "UnitPrice", "TaxRate" ,"PickedQuantity", "o.OrderID"]
df_fact_orders = df_fact_orders.alias("o").join(df_customer.alias("c"), on = col('o.CustomerID') == col('c.WWI_Customer_ID'), how = "left" ).select(selectedColumns)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

selectedColumns = ["PickingCompletedWhen", "Customer_Key", "Description", "Quantity", "UnitPrice", "TaxRate" ,"PickedQuantity", "OrderID", "od.Date as Order_Date"]
df_fact_orders = df_fact_orders.alias("o").join(df_date.alias("od"),on = col('o.OrderDate') == col('od.Date'), how = "left"  ).selectExpr(selectedColumns)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

selectedColumns = ["op.Date as Picking_Date", "Customer_Key", "Description", "Quantity", "UnitPrice", "TaxRate" ,"PickedQuantity", "OrderID", "Order_Date"]
df_fact_orders = df_fact_orders.alias("o").join(df_date.alias("op"),on = col('o.PickingCompletedWhen') == col('op.Date'), how = "left"  ).selectExpr(selectedColumns)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_fact_orders = df_fact_orders.withColumn('Order_Fullfillment_Time', datediff('Picking_Date', 'Order_Date'))
                               

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_fact_orders.write.format('delta').mode('overwrite').saveAsTable('GoldLayer.FactOrders_Temp')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("""MERGE INTO GoldLayer.FactOrders as TGT
    USING GoldLayer.FactOrders_Temp as SRC 
        ON TGT.Customer_Key = SRC.Customer_Key
            AND TGT.Picking_Date = SRC.Picking_Date
            AND TGT.Description = SRC.Description
            AND TGT.Order_Date = SRC.Order_Date
            AND TGT.OrderID = SRC.OrderID
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
 """)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("""DROP TABLE GoldLayer.factorders_temp""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
