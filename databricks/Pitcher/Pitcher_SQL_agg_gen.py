# Databricks notebook source
pip install openpyxl


# COMMAND ----------

dbutils.widgets.text("p_table_list_string","Account,AccountTeamMember,Affiliation__c,Brand__c,Event,HCO_Segmentation__c,Key_Message__c,PCH__Call_Expense__c,PCH__Call_Expense_Item__c,PCH__Call_Expense_Recipient__c,PCH__Congress__c,PCH__Congress_Call_Attendee__c,PCH__Congress_Call2__c,PCH__Congress_Expense__c,PCH__Congress_Expense_Type__c,PCH__Congress_Invitee__c,PCH__Congress_Invitee_Expense__c,PCH__Congress_Speaker__c,PCH__Engagement_Plan__c,PCH__Engagement_Plan_Target__c,PCH__Engagement_Plan_Target_Channel__c,PCH__Inventory_Transaction__c,PCH__Inventory_Transaction_Item__c,PCH__Product_Limit_Summary__c,PCH__Product_Lot__c,PCH__Speaker__c,PCH__ToT_Daily_User_Summary__c,PCH__ToT_User_Activity_Line__c,PCH__Valuable_Item__c,PITCHER__Multichannel_Activity_Line__c,pit_ci__Area_Budget__c,pit_ci__Congress__c,pit_ci__Congress_Regional_Manager_Summary__c,pit_ci__Congress_Service__c,pit_ci__Congress_Service_Tax__c,pit_ci__Line_Budget__c,pit_ci__Pitcher_Congress_Invitee__c,pit_ci__Pitcher_Congress_Invitee_Service__c,pit_ci__Pitcher_Congress_Rep__c,pit_ci__Pitcher_Division_Budget__c,pit_ci__Service_Type__c,pit_ci__Travel_Agency__c,pit_q__Audit_Response__c,pit_q__Audit_Response_Item__c,PITCHER__Multichannel_Activity__c,PITCHER__Pitcher_Activity__c,PITCHER__Pitcher_Attendee__c,PITCHER__Pitcher_Content__c,PITCHER__Pitcher_Presentation__c,PITCHER__Sent_Message__c,Pitcher_Brands_Discussed__c,Pitcher_Key_Messages_Discussed__c,Pitcher_Products_Discussed__c,PITCM__Consent_Management__c,Product2,Segment__c,Territories__c,Territory__c,User")
dbutils.widgets.text("p_secret_scope","DAN-WW-T-KVT800-R-CDS-DB")
dbutils.widgets.text("p_service_credential_key","SPN-ONESOURCE-WW-T-CDS-ANL")
dbutils.widgets.text("p_application_id","bb7086c6-32c0-43bd-bf70-98a20e5f9a8c")
dbutils.widgets.text("p_directory_id","4720ed5e-c545-46eb-99a5-958dd333e9f2")
dbutils.widgets.text("p_storage_account","daneutstashubjvkwicdxdzl")

### Import libraries 
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, col, current_timestamp,substring
from pyspark.sql.functions import col, substring
from pyspark.sql.types import StructType,StructField, StringType

import pandas as pd
import os
from pyspark.sql import SparkSession


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

# Remove extra files _success, _commit and _start
spark.conf.set("spark.sql.sources.commitProtocolClass",
               "org.apache.spark.sql.execution.datasources.SQLHadoopMapReduceCommitProtocol")
spark.conf.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")



#define url to expected data set
raw_adl_map_dir = "CDS_QA/Pitcher/Mapping/"
raw_adl_map_path = "abfss://raw@" + storage_account_name + ".dfs.core.windows.net/" + raw_adl_map_dir + "Pitcher_Mapping.xlsx" #to change for a correct file name


##Common obj to use
#list of objets to test (should be passed from ADF as a string)
table_list_string = dbutils.widgets.get("p_table_list_string")
#table into one string
table_list = table_list_string.split(",")

schema = StructType([StructField("text", StringType(), True)])



# Here is the problem on another clusters
local_file_path = "/tmp/Pitcher_Mapping.xlsx"
dbutils.fs.cp(raw_adl_map_path, "file://" + local_file_path) # <---- on my cluster is OK
mapping = pd.read_excel(local_file_path, engine='openpyxl')


# Generate select with agregation based on type of column. Will exetuce above csv files.
for t in table_list:
    pandas_df = mapping[mapping['table name'] == t].reset_index()

    target_query = "Select 'Profile' as Profile ,COUNT(*) as TOTAL_ROW_COUNT ,MAX(SystemModstamp) as MAX_MODIFICATION_DATE "
    for i in range(len(pandas_df)):
        # works with string
        if pandas_df['data type'].str.contains('Email|picklist|Phone|text|Url|Address|Combobox|Id|Lookup', case=False)[i]:
            target_query += f',SUM(LENGTH(' + pandas_df.loc[i, 'field name'] +')) as SUM_LEN_' + pandas_df.loc[i, 'field name'] + ''
            target_query += f',COUNT(DISTINCT(' + pandas_df.loc[i, 'field name'] +')) as COUNT_DISTINCT_' + pandas_df.loc[i, 'field name'] + ''
        
        #works with booleans
        elif pandas_df['data type'].str.contains('Boolean|Checkbox', case=False)[i]:
            target_query += f',SUM(CASE WHEN ' + pandas_df.loc[i, 'field name'] +' = "False" THEN 0 WHEN ' + pandas_df.loc[i, 'field name'] + ' = "True" THEN 1 ELSE '  + pandas_df.loc[i, 'field name'] + ' END) as SUM_' + pandas_df.loc[i, 'field name'] + ''
            target_query += f',COUNT(DISTINCT(' + pandas_df.loc[i, 'field name'] +')) as COUNT_DISTINCT_' + pandas_df.loc[i, 'field name'] + ''
        
        # works with numbers formulas
        elif pandas_df.loc[i, 'data type'] == 'Formula (Percent)' or pandas_df.loc[i, 'data type'] =='Formula (Currency)' or pandas_df.loc[i, 'data type']=='Formula (Number)' or pandas_df.loc[i, 'data type']=='Formula (Double)':
            target_query  += f',CAST(SUM(' + pandas_df.loc[i, 'field name'] + ') AS DECIMAL(30,2))  as SUM_' + pandas_df.loc[i, 'field name'] +''

        # works with numbers
        elif pandas_df['data type'].str.contains('Percent|Currency|Number|Double', case=False)[i]:
            ind1 = pandas_df['data type'][i].index('(')
            ind2 = pandas_df['data type'][i].index(')') + 1 #index start from 0
            target_query += f',CAST(SUM(' + pandas_df.loc[i, 'field name'] + ') AS DECIMAL' + pandas_df.loc[i, 'data type'][ind1:ind2] + ' )as SUM_' + pandas_df.loc[i, 'field name'] +''
           
        # works with time/datetime
        elif pandas_df['data type'].str.contains('Time|DateTime', case=False)[i]:
            target_query += f',COUNT(DISTINCT(' + pandas_df.loc[i, 'field name'] +')) as COUNT_DISTINCT_' + pandas_df.loc[i, 'field name'] + ''
            #yyyy-mm-dd hh-mm-ss
            target_query += f',SUM(LENGTH(SUBSTRING(' + pandas_df.loc[i, 'field name'] +' , 1 , 19))) as SUM_LEN_' + pandas_df.loc[i, 'field name'] + ''

        # works with date/date formula
        elif pandas_df['data type'].str.contains('Date', case=False)[i]:
            target_query += f',COUNT(DISTINCT(' + pandas_df.loc[i, 'field name'] +')) as COUNT_DISTINCT_' + pandas_df.loc[i, 'field name'] + ''
            #yyyy-mm-dd hh-mm-ss
            target_query += f',SUM(LENGTH(SUBSTRING(' + pandas_df.loc[i, 'field name'] +' , 1 , 10))) as SUM_LEN_' + pandas_df.loc[i, 'field name'] + ''

        else:
            target_query += f',COUNT(DISTINCT(' + pandas_df.loc[i, 'field name'] +')) as COUNT_DISTINCT_' + pandas_df.loc[i, 'field name'] + ''

    

    raw_adl_target_query_path = f"abfss://raw@" + storage_account_name + f".dfs.core.windows.net/CDS_QA/Pitcher/Tables/" + t + f"/SQL/ADL_AGG/" 

    df = spark.createDataFrame([(target_query,)], schema)
    df.coalesce(1).write.format("text").mode("overwrite").option("header", "false").save(raw_adl_target_query_path)

    files = dbutils.fs.ls(raw_adl_target_query_path)
    
   # Move each file to a new location or perform any desired operation
    for file in files:
       old_name = os.path.join(raw_adl_target_query_path, file.name)
       new_name = raw_adl_target_query_path + "SQL.txt"
       dbutils.fs.mv(old_name, new_name)

    # Generate select with agregation based on type of column. Will exetuce on the source side (Salesforce). Except LENGTH applied CHARACTER_LENGTH.
    source_query = "Select 'Profile' as Profile ,COUNT(*) as TOTAL_ROW_COUNT ,MAX(SystemModstamp) as MAX_MODIFICATION_DATE "
    for i in range(len(pandas_df)):
        # works with string
        if pandas_df['data type'].str.contains('Email|picklist|Phone|text|Url|Address|Combobox|Id|Lookup', case=False)[i]:
            source_query += f',SUM(LENGTH(' + pandas_df.loc[i, 'field name'] +')) as SUM_LEN_' + pandas_df.loc[i, 'field name'] + ''
            source_query += f',COUNT(DISTINCT(' + pandas_df.loc[i, 'field name'] +')) as COUNT_DISTINCT_' + pandas_df.loc[i, 'field name'] + ''

        #works with booleans
        elif pandas_df['data type'].str.contains('Boolean|Checkbox', case=False)[i]:
            source_query += f',SUM(' + pandas_df.loc[i, 'field name'] +') as SUM_' + pandas_df.loc[i, 'field name'] + ''
            source_query += f',COUNT(DISTINCT(' + pandas_df.loc[i, 'field name'] +')) as COUNT_DISTINCT_' + pandas_df.loc[i, 'field name'] + ''

        # works with numbers
        elif pandas_df['data type'].str.contains('Percent|Currency|Number|Double', case=False)[i]:
            source_query += f',SUM(' + pandas_df.loc[i, 'field name'] + ')  as SUM_' + pandas_df.loc[i, 'field name'] +''

        # works with date/datetime
        elif pandas_df['data type'].str.contains('Date|Time|DateTime', case=False)[i]:
            source_query += f',COUNT(DISTINCT(' + pandas_df.loc[i, 'field name'] +')) as COUNT_DISTINCT_' + pandas_df.loc[i, 'field name'] + ''
            source_query += f',SUM(LENGTH(' + pandas_df.loc[i, 'field name'] +')) as SUM_LEN_' + pandas_df.loc[i, 'field name'] + ''

        else:
            source_query += f',COUNT(DISTINCT(' + pandas_df.loc[i, 'field name'] +')) as COUNT_DISTINCT_' + pandas_df.loc[i, 'field name'] + ''

    
    raw_adl_source_query_path = f"abfss://raw@" + storage_account_name + f".dfs.core.windows.net/CDS_QA/Pitcher/Tables/" + t + f"/SQL/SLSFRC_AGG/" 

    df = spark.createDataFrame([(source_query,)], schema)
    df.coalesce(1).write.format("text").mode("overwrite").option("header", "false").save(raw_adl_source_query_path)

    files = dbutils.fs.ls(raw_adl_source_query_path)
    
   # Move each file to a new location or perform any desired operation
    for file in files:
       old_name = os.path.join(raw_adl_source_query_path, file.name)
       new_name = raw_adl_source_query_path + "SQL.txt"
       dbutils.fs.mv(old_name, new_name)
       
