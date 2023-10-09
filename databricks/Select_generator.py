# Databricks notebook source
# MAGIC %pip install openpyxl
# MAGIC %pip install azure-storage-file-datalake

# COMMAND ----------

dbutils.widgets.text("p_table_list_string","ZVT002")
dbutils.widgets.text("p_secret_scope","DAN-WW-T-KVT800-R-CDS-DB")
dbutils.widgets.text("p_service_credential_key","SPN-ONESOURCE-WW-T-CDS-ANL")
dbutils.widgets.text("p_application_id","bb7086c6-32c0-43bd-bf70-98a20e5f9a8c")
dbutils.widgets.text("p_directory_id","4720ed5e-c545-46eb-99a5-958dd333e9f2")
dbutils.widgets.text("p_storage_account","daneutstashubjvkwicdxdzl")

### Import libraries 
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, col, current_timestamp
from pyspark.sql.types import StructType,StructField, StringType

import pandas as pd
from pyspark.sql import SparkSession
from azure.storage.filedatalake import DataLakeFileClient
from azure.storage.filedatalake import DataLakeServiceClient

# Create a Spark session
#spark = SparkSession.builder.getOrCreate()

### Define initial parameters
# Azure authentication parameters
secret_scope = dbutils.widgets.get("p_secret_scope")
service_credential_key = dbutils.widgets.get("p_service_credential_key")
application_id = dbutils.widgets.get("p_application_id")
directory_id = dbutils.widgets.get("p_directory_id")
storage_account_name = dbutils.widgets.get("p_storage_account")
Container = "raw"
service_credential = dbutils.secrets.get(scope=secret_scope, key=service_credential_key)

# Set spark configuration
spark.conf.set("fs.azure.account.auth.type." + storage_account_name + ".dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type." + storage_account_name + ".dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id." + storage_account_name + ".dfs.core.windows.net", application_id)
spark.conf.set("fs.azure.account.oauth2.client.secret." + storage_account_name + ".dfs.core.windows.net", service_credential)
spark.conf.set("fs.azure.account.oauth2.client.endpoint." + storage_account_name + ".dfs.core.windows.net", "https://login.microsoftonline.com/" + directory_id + "/oauth2/token")
spark.conf.set("spark.hadoop.mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")



#define url to expected data set
raw_adl_map_dir = "CDS_QA/Artemis/Mapping/"
raw_adl_map_path = "abfss://raw@" + storage_account_name + ".dfs.core.windows.net/" + raw_adl_map_dir + "Artemis_Mapping.xlsx" #to change for a correct file name


##Common obj to use
#list of objets to test (should be passed from ADF as a string)
table_list_string = dbutils.widgets.get("p_table_list_string")
#table into one string
table_list = table_list_string.split(",")



# Here is the problem on another clusters
local_file_path = "/tmp/Artemis_Mapping.xlsx"
dbutils.fs.cp(raw_adl_map_path, "file://" + local_file_path) # <---- on my cluster is OK
mapping = pd.read_excel(local_file_path, engine='openpyxl')


for t in table_list:
    pandas_df = mapping[mapping['TABNAME'] == t].reset_index()

    target_query = "Select 'Profile' as Profile ,COUNT(*) as TOTAL_ROW_COUNT ,MAX(EDW_MDF_TST) as MAX_MODIFICATION_DATE "
    for i in range(len(pandas_df)):
        # works with string
        if pandas_df.loc[i, 'DATATYPE'] in ['CHAR' , 'VARCHAR' , 'STRING' , 'NUMC' , 'RAW' , 'LRAW' , 'BOOL', 'CUKY', 'CLNT', 'UNIT', 'LANG']:
            target_query += f',SUM(LENGTH(' + pandas_df.loc[i, 'FIELDNAME'] +')) as SUM_LEN_' + pandas_df.loc[i, 'FIELDNAME'] + ''
            target_query += f',COUNT(DISTINCT(' + pandas_df.loc[i, 'FIELDNAME'] +')) as COUNT_DISTINCT_' + pandas_df.loc[i, 'FIELDNAME'] + ''
        # works with numbers
        elif pandas_df.loc[i, 'DATATYPE'] in ['DEC' , 'QUAN' , 'INT1' , 'INT2' , 'INT4' , 'INT8' , 'FLTP' , 'CURR']:
            target_query += f',CAST(SUM(' + pandas_df.loc[i, 'FIELDNAME'] + ') AS DECIMAL(30,' + str(pandas_df.loc[i, 'DECIMALS']) +')) as SUM_' + pandas_df.loc[i, 'FIELDNAME'] +''
        # works with date/datetime
        elif pandas_df.loc[i, 'DATATYPE'] in  ['DATS' , 'TIMS' , 'SECONDDATE']:
            target_query += f',COUNT(DISTINCT(' + pandas_df.loc[i, 'FIELDNAME'] +')) as COUNT_DISTINCT_' + pandas_df.loc[i, 'FIELDNAME'] + ''
            target_query += f',SUM(LENGTH(' + pandas_df.loc[i, 'FIELDNAME'] +')) as SUM_LEN_' + pandas_df.loc[i, 'FIELDNAME'] + ''
        else:
            target_query += f',COUNT(DISTINCT(' + pandas_df.loc[i, 'FIELDNAME'] +')) as COUNT_DISTINCT_' + pandas_df.loc[i, 'FIELDNAME'] + ''


    # Set your Azure Data Lake Storage account details
    
    # KEY 1 from Access Key of storage account
    account_key = "IqWR9eDXSPt29xdAvlbkk4j0nYIRbdt6qPhIzGn1/pdtC3XdkCfpY1U19JM2W8o7oufjx9+wjd5vw8cuDMCkwg=="
    directory_path = "CDS_QA/Artemis/Tables/"+ t + "/SQL/Target/"
    file_name = "SQL.txt"
    file_path = directory_path + file_name

    # Create a DataLakeServiceClient object
    connection_string = f"DefaultEndpointsProtocol=https;AccountName={storage_account_name};AccountKey={account_key};EndpointSuffix=core.windows.net"
    service_client = DataLakeServiceClient.from_connection_string(connection_string)


    # Create a file client and write the data to the file in ADLS
    file_client = service_client.get_file_client(Container, file_path)
    file_client.create_file()
    file_client.append_data(target_query, 0, len(target_query))
    file_client.flush_data(len(target_query))

    source_query = "Select 'Profile' as Profile ,COUNT(*) as TOTAL_ROW_COUNT ,MAX(EDW_MDF_TST) as MAX_MODIFICATION_DATE "
    for i in range(len(pandas_df)):
        # works with string
        if pandas_df.loc[i, 'DATATYPE'] in ['CHAR' , 'VARCHAR' , 'STRING' , 'NUMC' , 'RAW' , 'LRAW' , 'BOOL', 'CUKY', 'CLNT', 'UNIT', 'LANG']:
            source_query += f',SUM(CHARACTER_LENGTH(' + pandas_df.loc[i, 'FIELDNAME'] +')) as SUM_LEN_' + pandas_df.loc[i, 'FIELDNAME'] + ''
            source_query += f',COUNT(DISTINCT(' + pandas_df.loc[i, 'FIELDNAME'] +')) as COUNT_DISTINCT_' + pandas_df.loc[i, 'FIELDNAME'] + ''
        # works with numbers
        elif pandas_df.loc[i, 'DATATYPE'] in ['DEC' , 'QUAN' , 'INT1' , 'INT2' , 'INT4' , 'INT8' , 'FLTP' , 'CURR']:
            source_query += f',CAST(SUM(' + pandas_df.loc[i, 'FIELDNAME'] + ') AS DECIMAL(30,' + str(pandas_df.loc[i, 'DECIMALS']) +')) as SUM_' + pandas_df.loc[i, 'FIELDNAME'] +''
        # works with date/datetime
        elif pandas_df.loc[i, 'DATATYPE'] in  ['DATS' , 'TIMS' , 'SECONDDATE']:
            source_query += f',COUNT(DISTINCT(' + pandas_df.loc[i, 'FIELDNAME'] +')) as COUNT_DISTINCT_' + pandas_df.loc[i, 'FIELDNAME'] + ''
            source_query += f',SUM(CHARACTER_LENGTH(' + pandas_df.loc[i, 'FIELDNAME'] +')) as SUM_LEN_' + pandas_df.loc[i, 'FIELDNAME'] + ''
        else:
            source_query += f',COUNT(DISTINCT(' + pandas_df.loc[i, 'FIELDNAME'] +')) as COUNT_DISTINCT_' + pandas_df.loc[i, 'FIELDNAME'] + ''


    # Set your Azure Data Lake Storage account details
    
    # KEY 1 from Access Key of storage account
    account_key = "IqWR9eDXSPt29xdAvlbkk4j0nYIRbdt6qPhIzGn1/pdtC3XdkCfpY1U19JM2W8o7oufjx9+wjd5vw8cuDMCkwg=="
    directory_path = "CDS_QA/Artemis/Tables/"+ t + "/SQL/Source/"
    file_name = "SQL.txt"
    file_path = directory_path + file_name

    # Create a DataLakeServiceClient object
    connection_string = f"DefaultEndpointsProtocol=https;AccountName={storage_account_name};AccountKey={account_key};EndpointSuffix=core.windows.net"
    service_client = DataLakeServiceClient.from_connection_string(connection_string)


    # Create a file client and write the data to the file in ADLS
    file_client = service_client.get_file_client(Container, file_path)
    file_client.create_file()
    file_client.append_data(source_query, 0, len(source_query))
    file_client.flush_data(len(source_query))


