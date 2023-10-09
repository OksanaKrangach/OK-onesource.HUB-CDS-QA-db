# Databricks notebook source
dbutils.widgets.text("p_secret_scope","Default")
dbutils.widgets.text("p_service_credential_key","Default")
dbutils.widgets.text("p_application_id","Default")
dbutils.widgets.text("p_directory_id","Default")
dbutils.widgets.text("p_storage_account","Default")
dbutils.widgets.text("p_table_list_string","Default")

# COMMAND ----------

### Import libraries 
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, col, current_timestamp, count, when
from pyspark.sql.types import StructType,StructField, StringType, ArrayType
from pyspark.sql.window import Window
from datetime import datetime
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

##Common obj to use
#list of objets to test (should be passed from ADF as a string)
table_list_string = dbutils.widgets.get("p_table_list_string")
#table into one string
table_list = table_list_string.split(",")


#path to save results
raw_adl_result_path = "abfss://raw@" + storage_account + ".dfs.core.windows.net/CDS_QA/Artemis/ProfilingComparison" 

#Create Schema for the ResultDataFrame
schema = StructType([
  StructField('Table', StringType(), True),
  StructField('Status', StringType(), True),
  StructField('Details', StringType(), True),
  StructField('Expected rows', StringType(), True),
  StructField('Actual rows', StringType(), True),
  StructField('Expected columns', StringType(), True),
  StructField('Actual columns', StringType(), True),
  StructField('Data Comparison', StringType(), True),
  StructField('Mismatching columns', StringType(), True)
  ])

#Create empty ResultDataFrame
ResultDataFrame = spark.createDataFrame([], schema)

##Iteratively comparison expected and actual CSVs data
for table in table_list:
    #Reset variable for cases if data will not be found
    Error_Details = ''
    Error_Details2 = ''
    row_count_exp = -1
    row_count_act = -2
    exp_minus_act = -3
    mismatching_col = 'NA'
    col_count_exp = -4
    col_count_act = -5
    csv_exp_file_delimiter = ","
    csv_act_file_delimiter = ","
    
    current_date = datetime.now().strftime("%Y%m%d")
    #define url to actual data set
    raw_adl_act_dir = "CDS_QA/Artemis/Tables/" + table + "/TargetProfiling/"
    raw_adl_act_path = "abfss://raw@" + storage_account + ".dfs.core.windows.net/" + raw_adl_act_dir 
    
    #define url to expected data set
    raw_adl_exp_dir = "CDS_QA/Artemis/Tables/" + table + "/SourceProfiling/" + table + "*.csv"
    raw_adl_exp_path = "abfss://raw@" + storage_account + ".dfs.core.windows.net/" + raw_adl_exp_dir 

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

        
        #Columns count
        col_count_exp = len(df_exp.columns)
        col_count_act = len(df_act.columns)
        
        try:
        #Do except act from exp can be 0 if actual contains more rows without changes
            exp_minus_act = df_exp.exceptAll(df_act).count()
            if exp_minus_act == 0:
                exp_minus_act = 'Data matching'
            else:
                exp_minus_act = 'Data mismatching'

                #Make an array of columns that have mismatching data
                filtered_columns = []
                columns = df_exp.columns
                for column in columns:
                       is_equal = df_exp.select(column).first() == df_act.select(column).first()
                       if not is_equal:
                          filtered_columns.append(column)  

                #Convert to string to use it in csv result file. 
                mismatching_col = ','.join(filtered_columns)

        except:
            Error_Details2 = 'File Structure error'

        
        if row_count_exp == row_count_act and col_count_exp == col_count_act and exp_minus_act == 'Data matching'  and Error_Details == '' and Error_Details2 == '' :
            Status = 'Passed'
        else:
            Status = 'Failed'  
    else:
        Status = 'Actual or expected data not found'

    columns = ['Table','Status','Details', 'Expected rows', 'Actual rows','Expected columns','Actual columns','Data Comparison','Mismatching columns']
    newRow = spark.createDataFrame([(table,Status,Error_Details2,row_count_exp,row_count_act,col_count_exp,col_count_act,exp_minus_act,mismatching_col)], columns)
    ResultDataFrame = ResultDataFrame.union(newRow)

#ResultDataFrame.show()
ResultDataFrame.repartition(1)\
               .write.format("csv")\
               .option("header", "true")\
               .option("taskCompletionMode", "NO_SUCCESS_FILE")\
               .mode("append")\
               .save(raw_adl_result_path)






