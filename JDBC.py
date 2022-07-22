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
emperorDf = ( spark.read.format('jdbc')
             .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") 
             .option("url", azure_sql_url) 
             .option("dbtable", db_table) 
             .option("databaseName", database_name) 
             .option("accessToken", access_token)
             .option("encrypt", "true") 
             .load()
            )

