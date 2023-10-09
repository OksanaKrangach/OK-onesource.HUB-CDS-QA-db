# Databricks notebook source
# MAGIC %pip install openpyxl
# MAGIC %pip install pyspark pyxlsb

# COMMAND ----------

dbutils.widgets.text("p_table_list_string","R_PTS_TER_2ND")
dbutils.widgets.text("p_secret_scope","DAN-WW-T-KVT800-R-CDS-DB")
dbutils.widgets.text("p_service_credential_key","SPN-ONESOURCE-WW-T-CDS-ANL")
dbutils.widgets.text("p_application_id","bb7086c6-32c0-43bd-bf70-98a20e5f9a8c")
dbutils.widgets.text("p_directory_id","4720ed5e-c545-46eb-99a5-958dd333e9f2")
dbutils.widgets.text("p_storage_account","daneutstashubjvkwicdxdzl")

# COMMAND ----------



### Import libraries 
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, col, current_timestamp
from pyspark.sql.types import StructType,StructField, StringType

import pandas as pd
from pyspark.sql import SparkSession

################################################################### SCRIPT WORKS WITH CORPORATE MEMORY MAPPING DOCUMENT ###################################################################################

#################### for each file reader generator need file example extracted by DIF for create SQL for temtales #####################


# Create a Spark session
spark = SparkSession.builder.getOrCreate()

### Define initial parameters
# Azure authentication parameters
secret_scope = dbutils.widgets.get("p_secret_scope")
service_credential_key = dbutils.widgets.get("p_service_credential_key")
application_id = dbutils.widgets.get("p_application_id")
directory_id = dbutils.widgets.get("p_directory_id")
storage_account_name = dbutils.widgets.get("p_storage_account")
service_credential = dbutils.secrets.get(scope=secret_scope, key=service_credential_key)

# Set spark configuration
spark.conf.set("fs.azure.account.auth.type." + storage_account_name + ".dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type." + storage_account_name + ".dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id." + storage_account_name + ".dfs.core.windows.net", application_id)
spark.conf.set("fs.azure.account.oauth2.client.secret." + storage_account_name + ".dfs.core.windows.net", service_credential)
spark.conf.set("fs.azure.account.oauth2.client.endpoint." + storage_account_name + ".dfs.core.windows.net", "https://login.microsoftonline.com/" + directory_id + "/oauth2/token")
spark.conf.set("spark.hadoop.mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")



# Remove extra files _success, _commit and _start
spark.conf.set("spark.sql.sources.commitProtocolClass",
               "org.apache.spark.sql.execution.datasources.SQLHadoopMapReduceCommitProtocol")
spark.conf.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")



##Common obj to use
#list of objets to test (should be passed from ADF as a string)
table_list_string = dbutils.widgets.get("p_table_list_string")
#table into one string
table_list = table_list_string.split(",")
schema = StructType([StructField("text", StringType(), True)])


raw_adl_map_dir = "CDS_QA/Pitcher/Mapping/"
raw_adl_map_path = "abfss://raw@" + storage_account_name + ".dfs.core.windows.net/" + raw_adl_map_dir + "PitcherSF_DM_Dictionary.xlsx" #to change for a correct file name


#read mapping
local_file_path = "/tmp/PitcherSF_DM_Dictionary.xlsx"
dbutils.fs.cp(raw_adl_map_path, "file://" + local_file_path) # <---- on my cluster is OK

columns_to_read = ["source table name", "field label (human readable)", "source field name", "source data type", "source description", "source primary key", "target table name", "target field name","target data type", "target data type", "target scale", "target primary key","target default value","target index","PII mark"] 
fullmap = pd.read_excel(local_file_path, engine = 'openpyxl', sheet_name = 'Dictionary',usecols=columns_to_read)


#convert to spark DF
pitcher_mapping = spark.createDataFrame(fullmap)

csv_file_delimiter = "|$"

#specify environment 
if storage_account_name == 'daneutstashubjvkwicdxdzl':
    env = 'DEV'
else:
    env = 'QAT'

for table in table_list:

    src_table = pitcher_mapping.filter(pitcher_mapping['target table name'] == table).select('source table name').first()["source table name"] 
    raw_adl_file_dir = "pts/Z01/"+ src_table +"/" ####here need to place correct folder path for the fileexample to read headers in correct order
    raw_adl_file_path = "abfss://raw@" + storage_account_name + ".dfs.core.windows.net/" + raw_adl_file_dir  #to change for a correct file name

    # Filter the DataFrame by condition
    filtered_pitcher_mapping = pitcher_mapping.filter(pitcher_mapping['target table name'] == table)  
    
    #pitcher_mapping.show(100)

    df_file = spark.read \
            .format("csv") \
            .option("recursiveFileLookup", "true")\
            .option("header", "true")\
            .option("delimiter", csv_file_delimiter)\
            .option("charset", "UTF8")\
            .option("lineSep", "\u001E") \
            .load(raw_adl_file_path)\
            .limit(1)

    headers = df_file.columns
    
    # Convert the list to a list of tuples
    data_tuples = [(item,) for item in headers]

    # Define the schema with a single StringType column
    schema = ["HeaderName"]
    headers_df = spark.createDataFrame(data_tuples, schema)

    # Perform inner join
    joined_frame = headers_df.join(filtered_pitcher_mapping, headers_df["HeaderName"] == filtered_pitcher_mapping["source field name"], how="left") #change to left join to allow to create test table with all columns and corretly mapp source - target names

    finallist = joined_frame.toPandas()

    #check SNS
    sns = finallist["PII mark"].isin(["PII"]).any()


    #1 Generate SQL query to check column counts
    
    expected_col_count = len(headers)

    if sns:
        Col_Cnt_Query = "SELECT COUNT(DISTINCT SOURCE_FIELD) AS ACTUAL_COL_CNT \nFROM "+env+"_SNS_CDS.SNS_CDS_DWH.T_CDS_MTD_ITM \nWHERE SOURCE_TABLE = UPPER('S_PTS_"+src_table+"') \nMINUS\nSELECT " + str(expected_col_count) + " AS EXPECTED_COL_CNT"
    
    else:
        Col_Cnt_Query = "SELECT COUNT(DISTINCT SOURCE_FIELD) AS ACTUAL_COL_CNT \nFROM "+env+"_CDS.CDS_DWH.T_CDS_MTD_ITM \nWHERE SOURCE_TABLE = UPPER('S_PTS_"+src_table+"') \nMINUS\nSELECT " + str(expected_col_count) + " AS EXPECTED_COL_CNT"

    raw_adl_Col_Cnt_Query_path = f"abfss://raw@" + storage_account_name + f".dfs.core.windows.net/CDS_QA/Pitcher/Tables/" + src_table + f"/SQL/ADL_SNFLK_COL_CNT/" 
    df = spark.createDataFrame([(Col_Cnt_Query,)], schema)
    df.coalesce(1).write.format("text").mode("overwrite").option("header", "false").save(raw_adl_Col_Cnt_Query_path)
    ################################################################
    #2 Generate SQL query for duplicates check
    Duplicates_Query = "Select 'Duplicates',SRC_SYS_COD"
    key_columns = ''
    for column in range(len(finallist)):
        if finallist.loc[column, 'source primary key'] == 'primary':
            key_columns += f' ,'+ finallist.loc[column, 'target field name']
            Duplicates_Query += key_columns

    if sns:
        Duplicates_Query += f'\nFROM SNS_CDS_DWH.' + table + '\n GROUP BY 1,SRC_SYS_COD '+key_columns+'\n HAVING COUNT(*)> 1'
    
    else:
        Duplicates_Query += f'\nFROM CDS_DWH.' + table + '\n GROUP BY 1,SRC_SYS_COD '+key_columns+'\n HAVING COUNT(*)> 1'
    
    
    raw_adl_Duplicates_Query_path = f"abfss://raw@" + storage_account_name + f".dfs.core.windows.net/CDS_QA/Pitcher/Tables/" + src_table + f"/SQL/SNFLK_DUPLICATES/" 
    df = spark.createDataFrame([(Duplicates_Query,)], schema)
    df.coalesce(1).write.format("text").mode("overwrite").option("header", "false").save(raw_adl_Duplicates_Query_path)
    #print(Duplicates_Query)
    ############################################################
	#3 Generate query that will collect data fron ADL files to test tables CDS_STG_PTS_Z01.TEST_%Target table name
    if sns:
        read_query = 'create or replace table SNS_CDS_STG_PTS_Z01.TEST_' + table + ' AS \nSELECT\n metadata$filename as filename\n' 
    else:
        read_query = 'create or replace table CDS_STG_PTS_Z01.TEST_' + table + ' AS \nSELECT\n metadata$filename as filename\n' 

    column_number = 1
    for column in range(len(finallist)):
        
        #strings
        if finallist.loc[column, "target data type"] in ["VARCHAR","STRING"]:
            read_query += f",NVL(TRIM($" + str(column_number) + "),'#' ) AS " + finallist.loc[column, "target field name"] + "\n"

        #boolean
        elif finallist.loc[column, "target data type"] in ["BOOLEAN"]:
            read_query += f",NVL(TRIM(LOWER($" + str(column_number) + ")),'false' ) AS " + finallist.loc[column, "target field name"] + "\n"
            
        #numbers
        elif finallist.loc[column, "target data type"] in ["DECIMAL", "SMALLINT", "NUMBER", "INTEGER", "BIGINT", "DOUBLE"]:
            read_query += f",NVL($" + str(column_number) + ",0) AS " + finallist.loc[column, "target field name"] + "\n"
        #dates
        elif finallist.loc[column, "target data type"] in ["DATE"]:
            read_query += f",NVL(TRIM($" + str(column_number) + "),'1000-01-01') AS " + finallist.loc[column, "target field name"] + "\n"
        #timestamps
        elif finallist.loc[column, "target data type"] in ["TIMESTAMP"] :
            read_query += f",NVL(TRIM($" + str(column_number) + "),'1000-01-01 00:00:00.000') AS " + finallist.loc[column, "target field name"] + "\n"
        
        else:
            read_query += f",NVL(TRIM($" + str(column_number) + "),'#' ) AS  Unknown_Column_"  + str(column_number) +  "\n"
        column_number += 1
    read_query += f",row_number() over (partition by 'TEST'" + key_columns + " order by SYS_MST_DAT desc, filename desc) as cnt\n"

    if sns:
      read_query += f'FROM @'+env+'_SNS_CDS.SNS_CDS_DWH.CDS_PTS_STAGE/Z01/' + src_table + '/''\n(file_format => "SNS_CDS_DWH".CDS_PTS_CSV_FORMAT, PATTERN => ".*.csv") WHERE METADATA$FILE_ROW_NUMBER > 1'
    else:
      read_query += f'FROM @'+env+'_CDS.CDS_DWH.CDS_PTS_STAGE/Z01/' + src_table + '/''\n(file_format => "CDS_DWH".CDS_PTS_CSV_FORMAT, PATTERN => ".*.csv") WHERE METADATA$FILE_ROW_NUMBER > 1'

    raw_adl_reader_path = f"abfss://raw@" + storage_account_name + f".dfs.core.windows.net/CDS_QA/Pitcher/Tables/" + src_table + f"/SQL/ADL_Reader/" 
    df = spark.createDataFrame([(read_query,)], schema)
    df.coalesce(1).write.format("text").mode("overwrite").option("header", "false").save(raw_adl_reader_path)
    #print(read_query)
    ################################################################
    #4 Generate query for row counts comparison
    if sns:
       row_counts = 'select count(*) as "counts" \n from SNS_CDS_STG_PTS_Z01.TEST_' + table + ' where cnt = 1 except select count(*) as "counts" from SNS_CDS_DWH.' + table
    else:
       row_counts = 'select count(*) as "counts" \n from CDS_STG_PTS_Z01.TEST_' + table + ' where cnt = 1 except select count(*) as "counts" from CDS_DWH.' + table

    raw_adl_row_counts_path = f"abfss://raw@" + storage_account_name + f".dfs.core.windows.net/CDS_QA/Pitcher/Tables/" + src_table + f"/SQL/ADL_SNFLK_COUNTS/" 
    df = spark.createDataFrame([(row_counts,)], schema)
    df.coalesce(1).write.format("text").mode("overwrite").option("header", "false").save(raw_adl_row_counts_path)
    #print(row_counts)
    ################################################################
    #5 Generate query for except operation between ADL and snowflake
    
    ##Filterout NAN values of target fileds in case of extra columns in files
    finallist = finallist.dropna(subset=['target field name']).reset_index()

    actual_expected_except = "select 'ACTUAL'"
    for column in range(len(finallist)):
        actual_expected_except += '\n,' + finallist.loc[column, 'target field name']
    if sns:
       actual_expected_except += '\nFROM SNS_CDS_DWH.' + table + '\n except \n'
    else:
       actual_expected_except += '\nFROM CDS_DWH.' + table + '\n except \n'

    actual_expected_except += "select 'ACTUAL'"
    for column in range(len(finallist)):
        actual_expected_except += '\n,' + finallist.loc[column, 'target field name']
    if sns:
       actual_expected_except += '\nFROM SNS_CDS_STG_PTS_Z01.TEST_' + table + '\n where cnt = 1\n union all \n'
    else:
       actual_expected_except += '\nFROM CDS_STG_PTS_Z01.TEST_' + table + '\n where cnt = 1\n union all \n'
    actual_expected_except += "select 'EXPECTED'"
    for column in range(len(finallist)):
        actual_expected_except += '\n,' + finallist.loc[column, 'target field name']
    if sns:
       actual_expected_except += '\nFROM SNS_CDS_STG_PTS_Z01.TEST_' + table + '\n where cnt = 1 \n except \n'
    else:
        actual_expected_except += '\nFROM CDS_STG_PTS_Z01.TEST_' + table + '\n where cnt = 1 \n except \n'
       
    actual_expected_except += "select 'EXPECTED'"
    for column in range(len(finallist)):
        actual_expected_except += '\n,' + finallist.loc[column, 'target field name']
    if sns:
      actual_expected_except += '\nFROM SNS_CDS_DWH.' + table
    else:
      actual_expected_except += '\nFROM CDS_DWH.' + table

    raw_adl_actual_expected_except_path = f"abfss://raw@" + storage_account_name + f".dfs.core.windows.net/CDS_QA/Pitcher/Tables/" + src_table + f"/SQL/ADL_SNFLK_EXCEPT/" 
    df = spark.createDataFrame([(actual_expected_except,)], schema)
    df.coalesce(1).write.format("text").mode("overwrite").option("header", "false").save(raw_adl_actual_expected_except_path)
    #print (actual_expected_except)
    ##############################################################


# COMMAND ----------


