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

entityName = 'customer_oltp'
finalEntityName = 'Customer'
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

schema_dimensions = {
    "customer_oltp": StructType([
        StructField('CustomerID', IntegerType(), True),
        StructField('CustomerName', StringType(), True),
        StructField('BillToCustomerID', IntegerType(), True),
        StructField('CustomerCategoryID', IntegerType(), True),
        StructField('BuyingGroupID', IntegerType(), True),
        StructField('PrimaryContactPersonID', IntegerType(), True),
        StructField('AlternateContactPersonID', StringType(), True),
        StructField('DeliveryMethodID', IntegerType(), True),
        StructField('DeliveryCityID', IntegerType(), True),
        StructField('PostalCityID', IntegerType(), True),
        StructField('CreditLimit', StringType(), True),
        StructField('AccountOpenedDate', DateType(), True),
        StructField('StandardDiscountPercentage', DoubleType(), True),
        StructField('IsStatementSent', IntegerType(), True),
        StructField('IsOnCreditHold', IntegerType(), True),
        StructField('PaymentDays', IntegerType(), True),
        StructField('PhoneNumber', StringType(), True),
        StructField('FaxNumber', StringType(), True),
        StructField('DeliveryRun', StringType(), True),
        StructField('RunPosition', StringType(), True),
        StructField('WebsiteURL', StringType(), True),
        StructField('DeliveryAddressLine1', StringType(), True),
        StructField('DeliveryAddressLine2', StringType(), True),
        StructField('DeliveryPostalCode', IntegerType(), True),
        StructField('DeliveryLocation', StringType(), True),
        StructField('PostalAddressLine1', StringType(), True),
        StructField('PostalAddressLine2', StringType(), True),
        StructField('PostalPostalCode', IntegerType(), True),
        StructField('LastEditedBy', IntegerType(), True),
        StructField('ValidFrom', TimestampType(), True),
        StructField('ValidTo', TimestampType(), True)
    ]),
    "buying_group" : StructType([
        StructField('BuyingGroupId', IntegerType(), True),
        StructField('BuyingGroupName', StringType(), True),
        StructField('LastEditedBy', IntegerType(), True),
        StructField('ValidFrom', TimestampType(), True),
        StructField('ValidTo', TimestampType(), True)
        ]),
    "customer_categories": StructType([
        StructField('CustomerCategoryID', IntegerType(), True),
        StructField('CustomerCategoryName', StringType(), True),
        StructField('LastEditedBy', IntegerType(),True ),
        StructField('ValidFrom', TimestampType(), True),
        StructField('ValidTo', TimestampType(), True)
    ]),
    "customer_info": StructType([
        StructField('CustomerID', IntegerType(), True),
        StructField('CustomerName', StringType(), True),
        StructField('CustomerCategoryName', StringType(), True),
        StructField('PrimaryContact', StringType(), True),
        StructField('AlternateContact', StringType(), True),
        StructField('PhoneNumber', StringType(), True),
        StructField('FaxNumber', StringType(), True),
        StructField('BuyingGroupName', StringType(), True),
        StructField('WebsiteURL', StringType(), True),
        StructField('DeliveryMethod', StringType(), True),
        StructField('CityName', StringType(), True),
        StructField('DeliveryLocation', StringType(), True),
        StructField('DeliveryRun', StringType(), True),
        StructField('RunPosition', StringType(), True)
    ]),
    "date" : StructType([
        StructField('Date', DateType(), True),
        StructField('Day_Number', IntegerType(), True),
        StructField('Day', IntegerType(), True),
        StructField('Month', StringType(), True),
        StructField('Short_Month', StringType(), True),
        StructField('Calendar_Month_Number', IntegerType(), True),
        StructField('Calendar_Month_Label', StringType(), True),
        StructField('Calendar_Year', IntegerType(), True),
        StructField('Calendar_Year_Label', StringType(), True)
    ])
}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_entity_raw = spark.read.format('csv').option("header", True).schema(schema_dimensions[entityName]).load(entityPath)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if (checkRowCount(df_entity_raw)):
    df_entity_raw.write.format("delta").mode('overwrite').saveAsTable(entityName)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
