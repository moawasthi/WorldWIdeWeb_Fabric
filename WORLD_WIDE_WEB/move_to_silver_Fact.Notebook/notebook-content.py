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

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, DoubleType, TimestampType

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# PARAMETERS CELL ********************

entityName = 'orderLine_oltp'
finalEntityName = 'Orders'
fileExtension = 'csv'

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

entityPath = 'abfss://WorldWideImporters@onelake.dfs.fabric.microsoft.com/BronzeLayer.Lakehouse/Files/' + finalEntityName + '/' + entityName + '.' + fileExtension

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def checkRowCount(df):
    return df.count() > 0

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_fact_bronze = spark.read.format('csv').option("header", True).option("inferschema",True).load(entityPath)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print(df_fact_bronze.schema)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

schema_facts = { "orders_oltp" : StructType([
    StructField('OrderID', IntegerType(), True), 
    StructField('CustomerID', IntegerType(), True), 
    StructField('SalespersonPersonID', IntegerType(), True), 
    StructField('PickedByPersonID', StringType(), True), 
    StructField('ContactPersonID', IntegerType(), True), 
    StructField('BackorderOrderID', StringType(), True), 
    StructField('OrderDate', DateType(), True), 
    StructField('ExpectedDeliveryDate', DateType(), True), 
    StructField('CustomerPurchaseOrderNumber', IntegerType(), True), 
    StructField('IsUndersupplyBackordered', IntegerType(), True), 
    StructField('Comments', StringType(), True), 
    StructField('DeliveryInstructions', StringType(), True), 
    StructField('InternalComments', StringType(), True), 
    StructField('PickingCompletedWhen', StringType(), True), 
    StructField('LastEditedBy', IntegerType(), True), 
    StructField('LastEditedWhen', TimestampType(), True)
    ]),
    "orderLine_oltp" : StructType([
        StructField('OrderLineID', IntegerType(), True),
        StructField('OrderID', IntegerType(), True), 
        StructField('StockItemID', IntegerType(), True), 
        StructField('Description', StringType(), True), 
        StructField('PackageTypeID', IntegerType(), True), 
        StructField('Quantity', IntegerType(), True), 
        StructField('UnitPrice', DoubleType(), True), 
        StructField('TaxRate', DoubleType(), True), 
        StructField('PickedQuantity', IntegerType(), True), 
        StructField('PickingCompletedWhen', StringType(), True), 
        StructField('LastEditedBy', IntegerType(), True), 
        StructField('LastEditedWhen', TimestampType(), True)
    ])
}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_fact_bronze = spark.read.format('csv').option("header", True).schema(schema_facts[entityName]).load(entityPath)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if (checkRowCount(df_fact_bronze)):
    df_fact_bronze.write.format("delta").mode('overwrite').saveAsTable(entityName)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
