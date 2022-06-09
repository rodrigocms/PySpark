#Create List From DF

def dfCol_to_list(df,col_to_list):   
  df = df.select(col_to_list)
  output_list=[]

  for i in df.collect():
    str = i[0]
    output_list.append(str)
  return output_list  
  
list_tables = dfCol_to_list(df_table,'dbase_tableName')
list_tables


#Create JSON file from list
for blob in blob_list_c1:
       item = {"blob_table":blob.name,"blob_table_id":blob.name.split('/')[1],"blob_table_id":blob.size }
       data.append(item)

json_list_c1 = json.dumps(data)


# Rename Months
df_reRun_2 = (df_comp_status        
            .withColumn('RestartProcess', regexp_extract(col('PARQUET_FILE_NAME_M'),r'(.+_\d{4}_\d+)',1))
            .withColumn('RestartProcess', regexp_replace('RestartProcess','_1','_01'))
            .withColumn('RestartProcess', regexp_replace('RestartProcess','_2','_02'))
            .withColumn('RestartProcess', regexp_replace('RestartProcess','_3','_03'))
            .withColumn('RestartProcess', regexp_replace('RestartProcess','_4','_04'))
            .withColumn('RestartProcess', regexp_replace('RestartProcess','_5','_05'))
            .withColumn('RestartProcess', regexp_replace('RestartProcess','_6','_06'))
            .withColumn('RestartProcess', regexp_replace('RestartProcess','_7','_07'))
            .withColumn('RestartProcess', regexp_replace('RestartProcess','_8','_08'))
            .withColumn('RestartProcess', regexp_replace('RestartProcess','_9','_09'))
            .withColumn('RestartProcess', regexp_replace('RestartProcess','_010','_10'))
            .withColumn('RestartProcess', regexp_replace('RestartProcess','_011','_11'))
            .withColumn('RestartProcess', regexp_replace('RestartProcess','_012','_12'))            
            .withColumn('RestartProcess', regexp_replace('RestartProcess','_020','_20'))          
            .select('RestartProcess')
            .dropDuplicates()            )
            

# CREATE DF FROM HDFS ls
from pyspark.sql.functions import *
from pyspark.sql.types import *

from subprocess import Popen, PIPE

hdfs_path = '/data/source/DB_LUSDW/oracletablesbackup/'

process = Popen('hdfs dfs -ls -R {}'.format(hdfs_path), shell=True, stdout=PIPE, stderr=PIPE)
std_out, std_err = process.communicate()
dir_list = std_out.decode("utf-8").split('\n')
dir_list = dir_list[1:len(dir_list)-1]

df_hdfs2= spark.createDataFrame(dir_list, StringType())



