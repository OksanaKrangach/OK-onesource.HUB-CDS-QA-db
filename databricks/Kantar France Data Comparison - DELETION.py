# Databricks notebook source
## This notebook is used to check data row by row for F-Tables. It is based on that when a record gets updated, its old version is deleted from the source. Each cell has one object.

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
from pyspark.sql.window import Window
from pyspark.sql.functions import rank, col, current_timestamp, count, when

## Common obj to use
#list of objets to test (should be passed from ADF as a string)
table_list_string = dbutils.widgets.get("p_table_list_string")
#table into one string
table_list = table_list_string.split(",")


## MSSQL Connection  QAT
driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
table = "DANONEFRUATOutbound.vwAnomalies" # to be changed 
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
raw_adl_result_path = "abfss://raw@" + storage_account + ".dfs.core.windows.net/CDS_QA/KantarFrance/DataComparison/Deletion/"+ table + "/" + datetime.now().strftime("%Y%m%d")

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

table_schema = dbutils.widgets.get("p_table_schema")
current_date = datetime.now().strftime("%Y/%m/%d") #only for Ref tables and can also be used with F tables in init mode only, for incr mode should check all folders vs source


for table in ['vwAnomalies']:

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
       raw_adl_act_dir = "KANTAR_XTEL/FRA/" + table + "/" 
      

       raw_adl_act_path = "abfss://raw@" + storage_account + ".dfs.core.windows.net/" + raw_adl_act_dir  
       print(raw_adl_act_path)
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
        print(Error_Details)
    print(Error_Details)
    if Error_Details != 'Data not found': 

        ## Some tests
        #Do rows count comparison
        row_count_exp = df_exp.count()
        
        # Define the window specification
        window_spec = Window.partitionBy("promoid", "promoproductanomid", "timeid", "idaction", "salesorgcode","UNIQUEKEY") \
                    .orderBy(
                       col("DTEMOD").desc()
                    )

        # Apply the window function and filter by row number
        df_dist_act = df_act.select('promoid', 'promoproductanomid', 'timeid', 'idaction', 'salesorgcode', 'anomaly', 'anomalytype', 'anomalycheckset', 'anomalygroup', 'DTEMOD', 'UNIQUEKEY') \
           .withColumn("rank", rank().over(window_spec)) \
           .filter(col("rank") == 1) \
           .drop("rank")
        row_count_act = df_dist_act.count()
        
        
        #Columns count
        col_count_exp = len(df_exp.columns)
        col_count_act = len(df_act.columns)

        try:
        #Do except act from exp can be 0 if actual contains more rows without changes
            exp_minus_act = df_exp.exceptAll(df_dist_act).count()
            act_minus_exp = df_dist_act.exceptAll(df_exp).count()


        except:
            Error_Details2 = 'File Structure error'
            print(Error_Details2)
        

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
       

# COMMAND ----------

import pandas as pd
import pyodbc
from datetime import datetime
from pyspark.sql.types import StructType,StructField, StringType, ArrayType
from pyspark.sql.window import Window
from pyspark.sql.functions import rank, col, current_timestamp, count, when

## Common obj to use
#list of objets to test (should be passed from ADF as a string)
table_list_string = dbutils.widgets.get("p_table_list_string")
#table into one string
table_list = table_list_string.split(",")


## MSSQL Connection  QAT
driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
table = "DANONEFRUATOutbound.vwClaims" # to be changed 
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
raw_adl_result_path = "abfss://raw@" + storage_account + ".dfs.core.windows.net/CDS_QA/KantarFrance/DataComparison/Deletion/"+ table + "/" + datetime.now().strftime("%Y%m%d")

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

table_schema = dbutils.widgets.get("p_table_schema")
current_date = datetime.now().strftime("%Y/%m/%d") #only for Ref tables and can also be used with F tables in init mode only, for incr mode should check all folders vs source


for table in ['vwClaims']:

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
       raw_adl_act_dir = "KANTAR_XTEL/FRA/" + table + "/" 
      

       raw_adl_act_path = "abfss://raw@" + storage_account + ".dfs.core.windows.net/" + raw_adl_act_dir  
       print(raw_adl_act_path)
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
        print(Error_Details)
    print(Error_Details)
    if Error_Details != 'Data not found': 

        ## Some tests
        #Do rows count comparison
        row_count_exp = df_exp.count()
        
        

        # Define the window specification
        window_spec = Window.partitionBy("CustomerID","TimeID","PromoID","ProductID","PromoProductMechID","AMOUNTID","Document","SalesOrgCode","Aging","DocumentRow") \
                    .orderBy(
                       col("DTEMOD").desc()
                    )

        # Apply the window function and filter by row number
        df_dist_act = df_act.select('CustomerID', 'TimeID', 'PromoID', 'ProductID', 'PromoProductMechID', 'AMOUNTID', 'Activity', 'Document', 'SalesOrgCode', 'Invoice', 'InvoiceType', 'Supplier', 'DocYear', 'InvoiceDate', 'PaymentMod', 'DueDate', 'DocumentState', 'CodCurFrom', 'InvPriceID', 'NetAmount', 'VatAmount', 'MatchingType', 'MatchingAmount', 'MatchingPrizeId', 'SentDate', 'Flgfake', 'Flgcanceled', 'ClaimNature', 'Aging', 'DocumentRow', 'Owner', 'Broker', 'LevProduct', 'State', 'ERPDocumentCode', 'Tax1', 'Tax2', 'Tax3', 'ZStateCode', 'Payer', 'Aging Segment', 'AgingSegmentOrder', 'DTEMOD', 'NOTES') \
           .withColumn("rank", rank().over(window_spec)) \
           .filter(col("rank") == 1) \
           .drop("rank")
        row_count_act = df_dist_act.count()
        
        #display(df_dist_act)
        
        #Columns count
        col_count_exp = len(df_exp.columns)
        col_count_act = len(df_act.columns)

        try:
        #Do except act from exp can be 0 if actual contains more rows without changes
            exp_minus_act = df_exp.exceptAll(df_dist_act).count()
            act_minus_exp = df_dist_act.exceptAll(df_exp).count()
            
            display(df_exp.exceptAll(df_dist_act))
            display(df_dist_act.exceptAll(df_exp))

        except:
            Error_Details2 = 'File Structure error'
            print(Error_Details2)
        

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

# COMMAND ----------

import pandas as pd
import pyodbc
from datetime import datetime
from pyspark.sql.types import StructType,StructField, StringType, ArrayType
from pyspark.sql.window import Window
from pyspark.sql.functions import rank, col, current_timestamp, count, when

## Common obj to use
#list of objets to test (should be passed from ADF as a string)
table_list_string = dbutils.widgets.get("p_table_list_string")
#table into one string
table_list = table_list_string.split(",")


## MSSQL Connection  QAT
driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
table = "DANONEFRUATOutbound.vwFunds" # to be changed 
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
raw_adl_result_path = "abfss://raw@" + storage_account + ".dfs.core.windows.net/CDS_QA/KantarFrance/DataComparison/Deletion/"+ table + "/" + datetime.now().strftime("%Y%m%d")

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

table_schema = dbutils.widgets.get("p_table_schema")
current_date = datetime.now().strftime("%Y/%m/%d") #only for Ref tables and can also be used with F tables in init mode only, for incr mode should check all folders vs source


for table in ['vwFunds']:

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
       raw_adl_act_dir = "KANTAR_XTEL/FRA/" + table + "/" 
      

       raw_adl_act_path = "abfss://raw@" + storage_account + ".dfs.core.windows.net/" + raw_adl_act_dir  
       print(raw_adl_act_path)
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
        print(Error_Details)
    print(Error_Details)
    if Error_Details != 'Data not found': 

        ## Some tests
        #Do rows count comparison
        row_count_exp = df_exp.count()
        
        # Define the window specification
        window_spec = Window.partitionBy("timeid","customerid","productid","funddetailid","fundid","salesorgcode") \
                    .orderBy(
                       col("DTEMOD").desc()
                    )

        # Apply the window function and filter by row number
        df_dist_act = df_act.select('timeid', 'customerid', 'productid', 'funddetailid', 'fundid', 'idconstraint', 'funddes', 'type', 'startdate', 'enddate', 'salesorgcode', 'fundstate', 'statedes', 'configuration', 'initialamount', 'baseamount', 'rateperc', 'curfromcode', 'rateflag', 'levproduct', 'dtemod') \
           .withColumn("rank", rank().over(window_spec)) \
           .filter(col("rank") == 1) \
           .drop("rank")
        row_count_act = df_dist_act.count()
        
        
        #Columns count
        col_count_exp = len(df_exp.columns)
        col_count_act = len(df_act.columns)

        try:
        #Do except act from exp can be 0 if actual contains more rows without changes
            exp_minus_act = df_exp.exceptAll(df_dist_act).count()
            act_minus_exp = df_dist_act.exceptAll(df_exp).count()


        except:
            Error_Details2 = 'File Structure error'
            print(Error_Details2)
        

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
       

# COMMAND ----------

import pandas as pd
import pyodbc
from datetime import datetime
from pyspark.sql.types import StructType,StructField, StringType, ArrayType
from pyspark.sql.window import Window
from pyspark.sql.functions import rank, col, current_timestamp, count, when

## Common obj to use
#list of objects to test (should be passed from ADF as a string)
table_list_string = dbutils.widgets.get("p_table_list_string")
#table into one string
table_list = table_list_string.split(",")


## MSSQL Connection  QAT
driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
table = "DANONEFRUATOutbound.vwFundsDetail" # to be changed 
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
raw_adl_result_path = "abfss://raw@" + storage_account + ".dfs.core.windows.net/CDS_QA/KantarFrance/DataComparison/Deletion/"+ table + "/" + datetime.now().strftime("%Y%m%d")

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

table_schema = dbutils.widgets.get("p_table_schema")
current_date = datetime.now().strftime("%Y/%m/%d") #only for Ref tables and can also be used with F tables in init mode only, for incr mode should check all folders vs source


for table in ['vwFundsDetail']:

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
       raw_adl_act_dir = "KANTAR_XTEL/FRA/" + table + "/" 
      

       raw_adl_act_path = "abfss://raw@" + storage_account + ".dfs.core.windows.net/" + raw_adl_act_dir  
       print(raw_adl_act_path)
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
        print(Error_Details)
    print(Error_Details)
    if Error_Details != 'Data not found': 

        ## Some tests
        #Do rows count comparison
        row_count_exp = df_exp.count()
        
        # Define the window specification
        window_spec = Window.partitionBy('timeid','promoid','funddetailid','fundid','reason','salesorgcode','fundmovementid') \
                    .orderBy(
                       col("DTEMOD").desc()
                    )

        # Apply the window function and filter by row number
        df_dist_act = df_act.select('timeid', 'promoid', 'funddetailid', 'fundid', 'constraintdesc', 'idaction', 'reason', 'movementtypecode', 'movementtype', 'movementvalue', 'salesorgcode', 'flgremainder', 'flgnotcommitted', 'curfromcode', 'fundmovementid', 'desfund', 'destransaction', 'dtemovement', 'desaction', 'dtemod') \
           .withColumn("rank", rank().over(window_spec)) \
           .filter(col("rank") == 1) \
           .drop("rank")
        row_count_act = df_dist_act.count()
        
        
        #Columns count
        col_count_exp = len(df_exp.columns)
        col_count_act = len(df_act.columns)

        try:
        #Do except act from exp can be 0 if actual contains more rows without changes
            exp_minus_act = df_exp.exceptAll(df_dist_act).count()
            act_minus_exp = df_dist_act.exceptAll(df_exp).count()


        except:
            Error_Details2 = 'File Structure error'
            print(Error_Details2)
        

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

# COMMAND ----------

import pandas as pd
import pyodbc
from datetime import datetime
from pyspark.sql.types import StructType,StructField, StringType, ArrayType
from pyspark.sql.window import Window
from pyspark.sql.functions import rank, col, current_timestamp, count, when

## Common obj to use
#list of objets to test (should be passed from ADF as a string)
table_list_string = dbutils.widgets.get("p_table_list_string")
#table into one string
table_list = table_list_string.split(",")


## MSSQL Connection  QAT
driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
table = "DANONEFRUATOutbound.VWPROMOSALES" # to be changed 
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
raw_adl_result_path = "abfss://raw@" + storage_account + ".dfs.core.windows.net/CDS_QA/KantarFrance/DataComparison/Deletion/"+ table + "/" + datetime.now().strftime("%Y%m%d")

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

table_schema = dbutils.widgets.get("p_table_schema")
current_date = datetime.now().strftime("%Y/%m/%d") #only for Ref tables and can also be used with F tables in init mode only, for incr mode should check all folders vs source


for table in ['VWPROMOSALES']:

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
       raw_adl_act_dir = "KANTAR_XTEL/FRA/" + table + "/" 
      

       raw_adl_act_path = "abfss://raw@" + storage_account + ".dfs.core.windows.net/" + raw_adl_act_dir  
       print(raw_adl_act_path)
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
        print(Error_Details)
    print(Error_Details)
    if Error_Details != 'Data not found': 

        ## Some tests
        #Do rows count comparison
        row_count_exp = df_exp.count()
        
        # Define the window specification
        window_spec = Window.partitionBy('customerid','timeid','promoproductid','promoid','productid','versioncode') \
                    .orderBy(
                       col("DTEMOD").desc()
                    )

        # Apply the window function and filter by row number
        df_dist_act = df_act.select('customerid', 'timeid', 'promoproductid', 'promoid', 'productid', 'umfromcode', 'versioncode', 'curfromcode', 'levproduct', 'levcustomer', 'salesorgcode', 'noagg_meas1', 'noagg_meas2', 'qty_meas1', 'qty_meas2', 'qty_meas3', 'qty_meas4', 'qty_meas5', 'qty_meas6', 'amt_meas1', 'amt_meas2', 'amt_meas3', 'amt_meas4', 'amt_meas5', 'amt_meas6', 'amt_meas7', 'amt_meas8', 'amt_meas9', 'amt_meas10', 'amt_meas11', 'amt_meas12', 'codparent', 'amt_meas13', 'amt_meas14', 'amt_meas15', 'amt_meas16', 'amt_meas17', 'amt_meas18', 'dtemod', 'coddisplay', 'QTY_MEAS7', 'QTY_MEAS8', 'SkuStatus') \
           .withColumn("rank", rank().over(window_spec)) \
           .filter(col("rank") == 1) \
           .drop("rank")
        row_count_act = df_dist_act.count()
        
        #Columns count
        col_count_exp = len(df_exp.columns)
        col_count_act = len(df_act.columns)

        try:
        #Do except act from exp can be 0 if actual contains more rows without changes
            exp_minus_act = df_exp.exceptAll(df_dist_act).count()
            act_minus_exp = df_dist_act.exceptAll(df_exp).count()


        except:
            Error_Details2 = 'File Structure error'
            print(Error_Details2)
        

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
       

# COMMAND ----------

import pandas as pd
import pyodbc
from datetime import datetime
from pyspark.sql.types import StructType,StructField, StringType, ArrayType
from pyspark.sql.window import Window
from pyspark.sql.functions import rank, col, current_timestamp, count, when

## Common obj to use
#list of objets to test (should be passed from ADF as a string)
table_list_string = dbutils.widgets.get("p_table_list_string")
#table into one string
table_list = table_list_string.split(",")


## MSSQL Connection  QAT
driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
table = "DANONEFRUATOutbound.vwTargets" # to be changed 
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
raw_adl_result_path = "abfss://raw@" + storage_account + ".dfs.core.windows.net/CDS_QA/KantarFrance/DataComparison/Deletion/"+ table + "/" + datetime.now().strftime("%Y%m%d")

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

table_schema = dbutils.widgets.get("p_table_schema")
current_date = datetime.now().strftime("%Y/%m/%d") #only for Ref tables and can also be used with F tables in init mode only, for incr mode should check all folders vs source


for table in ['vwTargets']:

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
       print(df_exp.columns)
       
       #Read data from ADL
       #For Fact tables, read from all folders
       raw_adl_act_dir = "KANTAR_XTEL/FRA/" + table + "/" 
      

       raw_adl_act_path = "abfss://raw@" + storage_account + ".dfs.core.windows.net/" + raw_adl_act_dir  
       print(raw_adl_act_path)
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
        print(Error_Details)
    print(Error_Details)
    if Error_Details != 'Data not found': 

        ## Some tests
        #Do rows count comparison
        row_count_exp = df_exp.count()
        
        # Define the window specification
        window_spec = Window.partitionBy('timeid','customerid','productid','salesorgcode','scenariocode','dim3') \
                    .orderBy(
                       col("DTEMOD").desc()
                    )

        # Apply the window function and filter by row number
        df_dist_act = df_act.select('timeid', 'customerid', 'productid', 'umfromcode', 'curfromcode', 'salesorgcode', 'scenariocode', 'incrperiod', 'qty_meas1', 'amt_meas1', 'amt_meas2', 'amt_meas3', 'amt_meas4', 'amt_meas7', 'amt_meas8', 'qty_meas2', 'dtemod', 'dim3', 'amt_meas16') \
           .withColumn("rank", rank().over(window_spec)) \
           .filter(col("rank") == 1) \
           .drop("rank")
        row_count_act = df_dist_act.count()
        
        
        #Columns count
        col_count_exp = len(df_exp.columns)
        col_count_act = len(df_act.columns)

        try:
        #Do except act from exp can be 0 if actual contains more rows without changes
            exp_minus_act = df_exp.exceptAll(df_dist_act).count()
            act_minus_exp = df_dist_act.exceptAll(df_exp).count()


        except:
            Error_Details2 = 'File Structure error'
            print(Error_Details2)
        

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
       

# COMMAND ----------

import pandas as pd
import pyodbc
from datetime import datetime
from pyspark.sql.types import StructType,StructField, StringType, ArrayType
from pyspark.sql.window import Window
from pyspark.sql.functions import rank, col, current_timestamp, count, when

## Common obj to use
#list of objets to test (should be passed from ADF as a string)
table_list_string = dbutils.widgets.get("p_table_list_string")
#table into one string
table_list = table_list_string.split(",")


## MSSQL Connection  QAT
driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
table = "DANONEFRUATOutbound.vwTotalSales" # to be changed 
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
raw_adl_result_path = "abfss://raw@" + storage_account + ".dfs.core.windows.net/CDS_QA/KantarFrance/DataComparison/Deletion/"+ table + "/" + datetime.now().strftime("%Y%m%d")

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

table_schema = dbutils.widgets.get("p_table_schema")
current_date = datetime.now().strftime("%Y/%m/%d") #only for Ref tables and can also be used with F tables in init mode only, for incr mode should check all folders vs source


for table in ['vwTotalSales']:

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
       raw_adl_act_dir = "KANTAR_XTEL/FRA/" + table + "/" 
      

       raw_adl_act_path = "abfss://raw@" + storage_account + ".dfs.core.windows.net/" + raw_adl_act_dir  
       print(raw_adl_act_path)
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
        print(Error_Details)
    print(Error_Details)
    if Error_Details != 'Data not found': 

        ## Some tests
        #Do rows count comparison
        row_count_exp = df_exp.count()
        
        # Define the window specification
        window_spec = Window.partitionBy('timeid','customerid','productid','salesorgcode','scenariocode','dim3') \
                    .orderBy(
                       col("DTEMOD").desc()
                    )

        # Apply the window function and filter by row number
        df_dist_act = df_act.select('timeid', 'customerid', 'productid', 'umfromcode', 'curfromcode', 'salesorgcode', 'scenariocode', 'incrperiod', 'amt_meas1', 'amt_meas2', 'amt_meas3', 'amt_meas4', 'amt_meas5', 'amt_meas6', 'amt_meas7', 'amt_meas8', 'amt_meas9', 'amt_meas10', 'amt_meas11', 'amt_meas12', 'amt_meas13', 'amt_meas14', 'amt_meas15', 'qty_meas1', 'qty_meas2', 'dtemod', 'dim3', 'amt_meas16') \
           .withColumn("rank", rank().over(window_spec)) \
           .filter(col("rank") == 1) \
           .drop("rank")
        row_count_act = df_dist_act.count()
        
        
        #Columns count
        col_count_exp = len(df_exp.columns)
        col_count_act = len(df_act.columns)

        try:
        #Do except act from exp can be 0 if actual contains more rows without changes
            exp_minus_act = df_exp.exceptAll(df_dist_act).count()
            act_minus_exp = df_dist_act.exceptAll(df_exp).count()


        except:
            Error_Details2 = 'File Structure error'
            print(Error_Details2)
        

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
