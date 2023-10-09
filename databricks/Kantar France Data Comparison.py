# Databricks notebook source
## Notes:
## This notebook compares data row by row for R and F tables. It's based on that when a record is updated in F-table, its old version isn't deleted from the source.

# COMMAND ----------

dbutils.widgets.text("p_secret_scope","DAN-WW-T-KVT800-R-CDS-DB")
dbutils.widgets.text("p_service_credential_key","SPN-ONESOURCE-WW-T-CDS-ANL")
dbutils.widgets.text("p_application_id","bb7086c6-32c0-43bd-bf70-98a20e5f9a8c")
dbutils.widgets.text("p_directory_id","4720ed5e-c545-46eb-99a5-958dd333e9f2")
dbutils.widgets.text("p_storage_account","daneutstashubjvkwicdxdzl")
dbutils.widgets.text("p_table_list_string","vwCalendar,VWCONSSHIP,vwCurFrom,vwCurrency")
dbutils.widgets.text("p_table_schema","DANONEFRTSTOutbound")
dbutils.widgets.text("mssqlUsername","Default")
dbutils.widgets.text("mssqlPassword","Default")
dbutils.widgets.text("mssqlURL","Default")

# COMMAND ----------

import pandas as pd
import pyodbc
from datetime import datetime
from pyspark.sql.types import StructType,StructField, StringType, ArrayType

## Common obj to use
#list of objets to test (should be passed from ADF as a string)
table_list_string = dbutils.widgets.get("p_table_list_string")
#table into one string
table_list = table_list_string.split(",")


## MSSQL Connection  QAT
driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
#table = "DANONEFRUATOutbound.vwAnomalies" # to be changed 
user = dbutils.widgets.get("mssqlUsername") # to be changed 
password = dbutils.widgets.get("mssqlPassword") # to be changed 
url = dbutils.widgets.get("mssqlURL")

## Azure authentication parameters
secret_scope = dbutils.widgets.get("p_secret_scope")
service_credential_key = dbutils.widgets.get("p_service_credential_key")
application_id = dbutils.widgets.get("p_application_id")
directory_id = dbutils.widgets.get("p_directory_id")
storage_account = dbutils.widgets.get("p_storage_account")
service_credential = dbutils.secrets.get(scope=secret_scope, key=service_credential_key)

## Set spark configurations
# Set credentials (Service Principal) for Azure Data Lake
spark.conf.set("fs.azure.account.auth.type." + storage_account + ".dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type." + storage_account + ".dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id." + storage_account + ".dfs.core.windows.net", application_id)
spark.conf.set("fs.azure.account.oauth2.client.secret." + storage_account + ".dfs.core.windows.net", service_credential)
spark.conf.set("fs.azure.account.oauth2.client.endpoint." + storage_account + ".dfs.core.windows.net", "https://login.microsoftonline.com/" + directory_id + "/oauth2/token")

# Remove extra files _success, _commit and _start - in case of output write
spark.conf.set("spark.sql.sources.commitProtocolClass",
               "org.apache.spark.sql.execution.datasources.SQLHadoopMapReduceCommitProtocol")
spark.conf.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")

#path to save results
raw_adl_result_path = "abfss://raw@" + storage_account + ".dfs.core.windows.net/CDS_QA/KantarFrance/DataComparison/" + datetime.now().strftime("%Y%m%d")

#Create Schema for the ResultDataFrame
result_schema = StructType([
  StructField('Table', StringType(), True),
  StructField('Status', StringType(), True),
  StructField('Details', StringType(), True),
  StructField('Expected rows', StringType(), True),
  StructField('Actual rows', StringType(), True),
  StructField('Expected columns', StringType(), True),
  StructField('Actual columns', StringType(), True),
  StructField('Expected minus Actual count', StringType(), True),
  StructField('Actual minus Expected count', StringType(), True)
  ])

#Create empty ResultDataFrame
ResultDataFrame = spark.createDataFrame([], result_schema)


#Reset variable for cases if data will not be found
Error_Details = ''
Error_Details2 = ''
row_count_exp = -1
row_count_act = -2
exp_minus_act = -3
act_minus_exp = -4
col_count_exp = -5
col_count_act = -6

table_schema = 'DANONEFRUATOutbound' #dbutils.widgets.get("p_table_schema")
current_date = datetime.now().strftime("%Y/%m/%d") #only for Ref tables and can also be used with F tables in init mode only, for incr mode should check all folders vs source


for table in table_list:

    dbtable = table_schema + "." + table
    print(dbtable)
   
    try:

       #Read data from MSSQL   
       df_exp = (spark.read
         .format("jdbc")
         .option("driver", driver)
         .option("url", url)
         .option("dbtable", dbtable)
         .option("user", user)
         .option("password", password)
         .option("inferSchema", "true") \
         .option("mergeSchema", "true") \
         .load()
       )
       
       #Read data from ADL
       #For Fact tables, read from all folders
       F_Tables = ['VWPROMOSALES' , 'vwAnomalies', 'vwFunds', 'vwFundsDetail', 'vwTargets', 'vwTotalSales', 'vwClaims']
       if table in F_Tables:
          raw_adl_act_dir = "KANTAR_XTEL/FRA/" + table + "/" 
       else:
          raw_adl_act_dir = "KANTAR_XTEL/FRA/" + table + "/" +current_date 

       raw_adl_act_path = "abfss://raw@" + storage_account + ".dfs.core.windows.net/" + raw_adl_act_dir  
       
       schema = df_exp.schema
       df_act = spark.read \
                   .format("csv") \
                   .option("recursiveFileLookup", "true")\
                   .option("header", "true") \
                   .option("delimiter", "\t") \
                   .schema(schema) \
                   .load(raw_adl_act_path)
       
       
    except: #Handle error when files not found
        Error_Details = 'Data not found'
        
    
    if Error_Details != 'Data not found': 

        ## Some tests
        #Do rows count comparison
        row_count_exp = df_exp.count()
        row_count_act = df_act.count()

        
        #Columns count
        col_count_exp = len(df_exp.columns)
        col_count_act = len(df_act.columns)

        try:
        #Do except act from exp can be 0 if actual contains more rows without changes
            exp_minus_act = df_exp.exceptAll(df_act).count()
            act_minus_exp = df_act.exceptAll(df_exp).count()
              
            display(df_exp.exceptAll(df_act))
            display(df_act.exceptAll(df_exp))

        except:
            Error_Details2 = 'File Structure error'
            
        

        if row_count_exp == row_count_act and col_count_exp == col_count_act and exp_minus_act == 0 and act_minus_exp == 0  and Error_Details == '' and Error_Details2 == '' :
            Status = 'Passed'
        else:
            Status = 'Failed'  
    else:
        Status = 'Actual or expected data not found'

    columns = ['Table','Status','Details', 'Expected rows', 'Actual rows','Expected columns','Actual columns','Expected minus Actual count','Actual minus Expected count']
    newRow = spark.createDataFrame([(table,Status,Error_Details2,row_count_exp,row_count_act,col_count_exp,col_count_act,exp_minus_act,act_minus_exp)], columns)
    ResultDataFrame = ResultDataFrame.union(newRow)


ResultDataFrame.repartition(1)\
               .write.format("csv")\
               .option("header", "true")\
               .option("taskCompletionMode", "NO_SUCCESS_FILE")\
               .mode("append")\
               .save(raw_adl_result_path)
       
