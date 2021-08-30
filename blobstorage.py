#Creating Connection String
account_name="jvccolallpedls01"
connection_string = "DefaultEndpointsProtocol=https;AccountName={account_name};AccountKey={account_key};EndpointSuffix=core.windows.net".format(account_name=account_name,account_key=account_key)

#Create Dataframe - setting Schema
from pyspark.sql.types import StructType,StructField, StringType, DateType

schema = (StructType([
                StructField('Container',StringType(),True),
                StructField('Name',StringType(),True),
                StructField('last_modified',DateType(),True)
                ]))

df_blob_files_c1 = spark.createDataFrame(spark.sparkContext.emptyRDD(),schema)

# List Blob In Container
from azure.storage.blob import ContainerClient
from pyspark.sql.types import StructType,StructField, StringType, IntegerType
import json

blob_container = 'lusdw'

data=[]
container = ContainerClient.from_connection_string(conn_str=connection_string, container_name=blob_container)

blob_list_c1 = container.list_blobs(name_starts_with='tables/')

for blob in blob_list_c1:
       item = {"blob_table":blob.name,"blob_table_id":blob.name.split('/')[1],"blob_table_id":blob.size }
       data.append(item)

json_list_c1 = json.dumps(data)

#Creating Dataframe From json_list
df_c1 = spark.read.json(sc.parallelize([json_list_c1])).filter(col('blob_table').substr(-7,7)=='parquet' )



