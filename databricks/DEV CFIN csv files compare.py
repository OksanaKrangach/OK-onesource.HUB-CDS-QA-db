# Databricks notebook source
dbutils.widgets.text("p_secret_scope","Default")
dbutils.widgets.text("p_service_credential_key","Default")
dbutils.widgets.text("p_application_id","Default")
dbutils.widgets.text("p_directory_id","Default")
dbutils.widgets.text("p_storage_account","Default")

# COMMAND ----------

### Import libraries 
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, col, current_timestamp, count, when
from pyspark.sql.types import StructType,StructField, StringType
from pyspark.sql.window import Window
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

# Remove extra files _success, _commit and _start
spark.conf.set("spark.sql.sources.commitProtocolClass",
               "org.apache.spark.sql.execution.datasources.SQLHadoopMapReduceCommitProtocol")
spark.conf.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")

#path to save results
raw_adl_result_path = "abfss://raw@" + storage_account + ".dfs.core.windows.net/ttm/MSaleh_test/test_cfin/Results" 

#Create Schema for the ResultDataFrame
schema = StructType([
  StructField('Status', StringType(), True),
  StructField('Details', StringType(), True),
  StructField('Expected rows', StringType(), True),
  StructField('Actual rows', StringType(), True),
  StructField('Expected minus Actual count', StringType(), True),
  StructField('Actual minus Expected count', StringType(), True),
  StructField('Expected columns', StringType(), True),
  StructField('Actual columns', StringType(), True),
  StructField('Latest Distinct Expected Count', StringType(), True),
  StructField('Latest Distinct Actual Count', StringType(), True)
  ])

#Create empty ResultDataFrame
ResultDataFrame = spark.createDataFrame([], schema)


#Reset variable for cases if data will not be found
Error_Details = ''
Error_Details2 = ''
row_count_exp = -1
row_count_act = -2
exp_minus_act = -3
act_minus_exp = -3
col_count_exp = -4
col_count_act = -5
latest_dis_exp_cnt = -6
latest_dis_act_cnt = -6
csv_exp_file_delimiter = ","
csv_act_file_delimiter = ","

#define url to expected data set
raw_adl_exp_dir = "ttm/MSaleh_test/test_cfin/test1/2023/"
raw_adl_exp_path = "abfss://raw@" + storage_account + ".dfs.core.windows.net/" + raw_adl_exp_dir 

#define url to actual data set
raw_adl_act_dir = "ttm/MSaleh_test/test_cfin/test2/2023/"
raw_adl_act_path = "abfss://raw@" + storage_account + ".dfs.core.windows.net/" + raw_adl_act_dir 

#Check presence of expected and actual datasets on ADL
### Read CSV
# Read the expected CSV files into a PySpark DataFrame
try:
        
        df_exp = spark.read \
            .format("csv") \
            .option("recursiveFileLookup", "true")\
            .option("header", "true") \
            .option("delimiter", csv_exp_file_delimiter) \
            .option("inferSchema", "true") \
            .option("mergeSchema", "true") \
            .load(raw_adl_exp_path)

        df_act = spark.read \
            .format("csv") \
            .option("recursiveFileLookup", "true")\
            .option("header", "true") \
            .option("delimiter", csv_act_file_delimiter) \
            .option("inferSchema", "true") \
            .option("mergeSchema", "true") \
            .load(raw_adl_act_path)
except: #Handle error when files not found
        Error_Details = 'Data not found'

if Error_Details != 'Data not found':
        
        
        ## Some tests
        #Do rows count comparison
        row_count_exp = df_exp.count()
        row_count_act = df_act.count()

        #check duplicates for both datasets 
        # Define the window specification
        window_spec = Window.partitionBy("SourceLedger", "CompanyCode", "FiscalYear", "AccountingDocument", "LedgerGLLineItem") \
                    .orderBy(
                        when(col('/1DH/OPERATION') == "D", 1)
                        .when(col('/1DH/OPERATION') == "U", 2)
                        .when(col('/1DH/OPERATION') == "I", 3)
                        .otherwise(4)
                        .asc(),
                        col("LASTCHANGEDATETIME").desc()
                    )

        # Apply the window function and filter by row number
        df_dist_act = df_act.select("SourceLedger", "CompanyCode", "FiscalYear", "AccountingDocument", "LedgerGLLineItem","/1DH/OPERATION","LASTCHANGEDATETIME") \
           .withColumn("row_number", row_number().over(window_spec)) \
           .filter(col("row_number") == 1) \
           .drop("row_number")
        latest_dis_act_cnt = df_dist_act.count()

        df_dist_exp = df_exp.select("SourceLedger", "CompanyCode", "FiscalYear", "AccountingDocument", "LedgerGLLineItem","/1DH/OPERATION","LASTCHANGEDATETIME") \
           .withColumn("row_number", row_number().over(window_spec)) \
           .filter(col("row_number") == 1) \
           .drop("row_number")
        latest_dis_exp_cnt = df_dist_exp.count()


        #Columns count
        col_count_exp = len(df_exp.columns)
        col_count_act = len(df_act.columns)

        try:
        #Do except act from exp can be 0 if actual contains more rows without changes
            exp_minus_act = df_exp.exceptAll(df_act).count()
            act_minus_exp = df_act.exceptAll(df_exp).count()
        except:
            Error_Details2 = 'File Structure error'


        if row_count_exp == row_count_act and col_count_exp == col_count_act and exp_minus_act == 0 and act_minus_exp == 0 and Error_Details == '' and Error_Details2 == '' :
            Status = 'Passed'
        else:
            Status = 'Failed'  
else:
        Status = 'Actual or expected data not found'

columns = ['Status','Details', 'Expected rows', 'Actual rows', 'Expected minus Actual count','Actual minus Expected count', 'Expected columns','Actual columns','Latest Distinct Expected Count','Latest Distinct Actual Count']
newRow = spark.createDataFrame([(Status,Error_Details2,row_count_exp,row_count_act,exp_minus_act,act_minus_exp,col_count_exp,col_count_act,latest_dis_exp_cnt,latest_dis_act_cnt)], columns)
ResultDataFrame = ResultDataFrame.union(newRow)


ResultDataFrame.repartition(1)\
               .write.format("csv")\
               .option("header", "true")\
               .option("taskCompletionMode", "NO_SUCCESS_FILE")\
               .mode("append")\
               .save(raw_adl_result_path)






