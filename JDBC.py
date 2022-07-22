#mariadb
mdb_partitions2 = (spark.read
                .format("jdbc")
                .option("url", "jdbc:mysql://liscoldrm2.lusitania-cs.pt:3306/DEV")
                .option("driver", "com.mysql.jdbc.Driver")
                .option("dbtable", "SYS_TABLES_CLOUD_PANO_PMES")
                .option("user", "nifi")
                .option("password", nifi_password)
                .load()) 

# sql server
pbi_datamodel_server = 'yllvffwpdraetakrjkjl2r2o2a-wj4o7ey3drre3oaydgjwnapg3a.datamart.pbidedicated.windows.net'
azure_sql_url = f"jdbc:sqlserver://{pbi_datamodel_server}"

database_name = 'db_powerbiprodemea_20220526_07494958_5f15'
db_table = "model.dim_market" 

emperorDf = ( spark.read.format('jdbc')
             .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") 
             .option("url", azure_sql_url) 
             .option("dbtable", db_table) 
             .option("databaseName", database_name) 
             .option("accessToken", access_token)
             .option("encrypt", "true") 
             .load()
            )

