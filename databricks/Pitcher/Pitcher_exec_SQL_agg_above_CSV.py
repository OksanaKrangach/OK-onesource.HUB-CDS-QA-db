# Databricks notebook source
# MAGIC %pip install adlfs
# MAGIC

# COMMAND ----------

dbutils.widgets.text("p_table_list_string","Account,AccountTeamMember,Affiliation__c,Brand__c,Event,HCO_Segmentation__c,Key_Message__c,PCH__Call_Expense__c,PCH__Call_Expense_Item__c,PCH__Call_Expense_Recipient__c,PCH__Congress__c,PCH__Congress_Call_Attendee__c,PCH__Congress_Call2__c,PCH__Congress_Expense__c,PCH__Congress_Expense_Type__c,PCH__Congress_Invitee__c,PCH__Congress_Invitee_Expense__c,PCH__Congress_Speaker__c,PCH__Engagement_Plan__c,PCH__Engagement_Plan_Target__c,PCH__Engagement_Plan_Target_Channel__c,PCH__Inventory_Transaction__c,PCH__Inventory_Transaction_Item__c,PCH__Product_Limit_Summary__c,PCH__Product_Lot__c,PCH__Speaker__c,PCH__ToT_Daily_User_Summary__c,PCH__ToT_User_Activity_Line__c,PCH__Valuable_Item__c,PITCHER__Multichannel_Activity_Line__c,pit_ci__Area_Budget__c,pit_ci__Congress__c,pit_ci__Congress_Regional_Manager_Summary__c,pit_ci__Congress_Service__c,pit_ci__Congress_Service_Tax__c,pit_ci__Line_Budget__c,pit_ci__Pitcher_Congress_Invitee__c,pit_ci__Pitcher_Congress_Invitee_Service__c,pit_ci__Pitcher_Congress_Rep__c,pit_ci__Pitcher_Division_Budget__c,pit_ci__Service_Type__c,pit_ci__Travel_Agency__c,pit_q__Audit_Response__c,pit_q__Audit_Response_Item__c,PITCHER__Multichannel_Activity__c,PITCHER__Pitcher_Activity__c,PITCHER__Pitcher_Attendee__c,PITCHER__Pitcher_Content__c,PITCHER__Pitcher_Presentation__c,PITCHER__Sent_Message__c,Pitcher_Brands_Discussed__c,Pitcher_Key_Messages_Discussed__c,Pitcher_Products_Discussed__c,PITCM__Consent_Management__c,Product2,Segment__c,Territories__c,Territory__c,User")
dbutils.widgets.text("p_secret_scope","DAN-WW-T-KVT800-R-CDS-DB")
dbutils.widgets.text("p_service_credential_key","SPN-ONESOURCE-WW-T-CDS-ANL")
dbutils.widgets.text("p_application_id","bb7086c6-32c0-43bd-bf70-98a20e5f9a8c")
dbutils.widgets.text("p_directory_id","4720ed5e-c545-46eb-99a5-958dd333e9f2")
dbutils.widgets.text("p_storage_account","daneutstashubjvkwicdxdzl")



### Import libraries 
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, col, current_timestamp
from pyspark.sql.types import StructType,StructField, StringType


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

csv_act_file_delimiter = "|$" 

#path to save results


for t in table_list:
    #define url to actual data set
    # path for a csv
    raw_adl_act_dir = "pts/Z01/" + t + "/"
    raw_adl_act_path = "abfss://raw@" + storage_account + ".dfs.core.windows.net/" + raw_adl_act_dir 

    # path for an SQL file
    raw_adl_sql_dir = "CDS_QA/Pitcher/Tables/" + t + "/SQL/ADL_AGG/"
    raw_adl_sql_path = "abfss://raw@" + storage_account + ".dfs.core.windows.net/" + raw_adl_sql_dir + "SQL.txt" 

    raw_adl_result_path = "abfss://raw@" + storage_account + ".dfs.core.windows.net/CDS_QA/Pitcher/Tables/" + t + "/ADLProfiling/"
    
    file_contents = dbutils.fs.head(raw_adl_sql_path)
    
    try:
        df_act = spark.read \
            .format("csv") \
            .option("recursiveFileLookup", "true")\
            .option("header", "true")\
            .option("delimiter", csv_act_file_delimiter)\
            .option("charset", "UTF8")\
            .option("lineSep", "\u001E") \
            .load(raw_adl_act_path)
        
        
       
        df_act.createOrReplaceTempView("my_" + t)
        sql = file_contents + ' FROM (SELECT *,ROW_NUMBER() OVER (PARTITION BY Id ORDER BY SystemModStamp DESC) AS CNT FROM my_' + t + ') WHERE CNT = 1'

        result = spark.sql(sql)
        result.coalesce(1).write.mode("overwrite").csv(raw_adl_result_path, header=True, quote = '"', quoteAll = True)

        
        
    except:
        print( t + " Error")
