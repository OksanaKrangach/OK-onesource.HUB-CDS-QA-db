# Databricks notebook source
# MAGIC %pip install openpyxl
# MAGIC %pip install azure-storage-file-datalake

# COMMAND ----------

dbutils.widgets.text("p_table_list_string","EHSBT_APPZVT002L_SCOPE")
dbutils.widgets.text("p_secret_scope","DAN-WW-T-KVT800-R-CDS-DB")
dbutils.widgets.text("p_service_credential_key","SPN-ONESOURCE-WW-T-CDS-ANL")
dbutils.widgets.text("p_application_id","bb7086c6-32c0-43bd-bf70-98a20e5f9a8c")
dbutils.widgets.text("p_directory_id","4720ed5e-c545-46eb-99a5-958dd333e9f2")
dbutils.widgets.text("p_storage_account","daneutstashubjvkwicdxdzl")



### Import libraries 
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, col, current_timestamp
from pyspark.sql.types import StructType,StructField, StringType

from azure.storage.filedatalake import DataLakeFileClient
from azure.storage.filedatalake import DataLakeServiceClient

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("CSV SQL Example") \
    .getOrCreate()


### Define initial parameters
# Azure authentication parameters
secret_scope = dbutils.widgets.get("p_secret_scope")
service_credential_key = dbutils.widgets.get("p_service_credential_key")
application_id = dbutils.widgets.get("p_application_id")
directory_id = dbutils.widgets.get("p_directory_id")
storage_account = dbutils.widgets.get("p_storage_account")
service_credential = dbutils.secrets.get(scope=secret_scope, key=service_credential_key)

### Set spark configurations
# Set credentials (Service Principal) for Azure Data Lake
spark.conf.set("fs.azure.account.auth.type." + storage_account + ".dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type." + storage_account + ".dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id." + storage_account + ".dfs.core.windows.net", application_id)
spark.conf.set("fs.azure.account.oauth2.client.secret." + storage_account + ".dfs.core.windows.net", service_credential)
spark.conf.set("fs.azure.account.oauth2.client.endpoint." + storage_account + ".dfs.core.windows.net", "https://login.microsoftonline.com/" + directory_id + "/oauth2/token")
spark.conf.set("spark.sql.parquet.output.committer.class", "org.apache.hadoop.mapreduce.lib.output.DirectFileOutputCommitter")
spark.conf.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")



##Common obj to use
#list of objets to test (should be passed from ADF as a string)
table_list_string = dbutils.widgets.get("p_table_list_string")
#table into one string
table_list = table_list_string.split(",")

csv_act_file_delimiter = "," 

#path to save results


for t in table_list:
    #define url to actual data set
    # path for a csv
    raw_adl_act_dir = "art/Cybele/" + t + "/"
    raw_adl_act_path = "abfss://raw@" + storage_account + ".dfs.core.windows.net/" + raw_adl_act_dir + "*.csv" 

    # path for an SQL file
    raw_adl_sql_dir = "CDS_QA/Artemis/Tables/" + t + "/SQL/Target/"
    raw_adl_sql_path = "abfss://raw@" + storage_account + ".dfs.core.windows.net/" + raw_adl_sql_dir + "SQL.txt" 

    raw_adl_result_path = "abfss://raw@" + storage_account + ".dfs.core.windows.net/CDS_QA/Artemis/Tables/" + t + "/TargetProfiling/"

    file_contents = dbutils.fs.head(raw_adl_sql_path)

    #Print the file contents
    #print(file_contents)
    try:
        df_act = spark.read \
            .format("csv") \
            .option("header", "true") \
            .option("delimiter", csv_act_file_delimiter) \
            .option("quote",'"')\
            .option("quoteAll", "true")\
            .option("charset", "UTF8")\
            .load(raw_adl_act_path)


        df_act.createOrReplaceTempView("my_" + t)
        sql = file_contents + ' FROM my_' + t

        result = spark.sql(sql)
        result.coalesce(1).write.mode("overwrite").csv(raw_adl_result_path, header=True, quote = '"', quoteAll = True)
    except:
        print("Error")
