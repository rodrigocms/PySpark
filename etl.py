
#rename #select
mdb_partitions_p = (mdb_partitions
                            .withColumnRenamed('Datalake_Finish_TS','Datalake_Finish_TS_P') 
                            .filter(col('PARTITION_NAME').isNull())
                            .select('PARTITION_NAME_P','Datalake_Finish_TS_P','TABLE_NAME_P')
                            .dropDuplicates())


# Join 
conf_mariadb = mdb_fragments.join(mdb_partitions_p, mdb_fragments.PARTITION_NAME == mdb_partitions_p.PARTITION_NAME_P, 'full'  )          

#split #newcolumn #deletecolumn #regexp_replace #regexp_extract #substring #isNull 
df_cloud_c1 = (df_c1
                .withColumn('splited', split('blob_table','/'))
                .withColumn('TABLE_NAME_C1', split('blob_table','/')[1])
                .withColumn('PANO_C1', split('blob_table','/')[2].substr(-4,4))
                .withColumn('FRAGMENT_CREATION_TS_C1', col('FRAGMENT_CREATION_TS_C1').substr(0,8))
                .withColumn('FRAGMENT_ID_C1', split(regexp_replace(col('blob_table'),'(_\d{10}\d+_\d+.parquet)',''),'/')[4])
                .withColumn('PARTITION_NAME_C1', regexp_extract(col('PARQUET_FILE_NAME_C1'),r'(.+_\d{4}_\d+)(_\d+_)(\d{8})(\d{9})(_)(\d+.parquet)',1)) 
                .filter(col('PARQUET_FILE_NAME_C1').isNotNull())
                .drop('splited','blob_table')
                .select('PARQUET_FILE_NAME_C1','TABLE_NAME_C1','PANO_C1','PMES_C1','FRAGMENT_ID_C1','PARTITION_NAME_C1','FRAGMENT_CREATION_TS_C1')
            )

# if else (When) #order by
df_comp_status = (df_comp_mdb_DL_1
        .select('PARQUET_FILE_NAME_M','PARQUET_FILE_NAME_C1')#'TABLE_NAME_C','PANO_C','PMES_C','FRAGMENT_CREATION_TS_C','TABLE_NAME_M','PANO_M','PMES_M','FRAGMENT_CREATION_TS_M')
        .withColumn('Status',  when(col('PARQUET_FILE_NAME_M') ==col('PARQUET_FILE_NAME_C1') ,'Ok')
                                                .when((col('PARQUET_FILE_NAME_C1').isNull()) & (col('PARQUET_FILE_NAME_M').isNotNull()) ,' Nao Existe Na Cloud')
                                                .when((col('PARQUET_FILE_NAME_M').isNull()) & (col('PARQUET_FILE_NAME_C1').isNotNull()) ,' Nao Existe Na MariaDB')

                                                .otherwise(' Not Ok'))
        .orderBy(col('PARQUET_FILE_NAME_C1'), ascending=False)  
        .filter(col('Status')!='Ok'))