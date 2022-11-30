import pandas as pd
from simple_salesforce import bulk
from simple_salesforce import Salesforce as sf
from datetime import date
from datetime import datetime
import teradatasql
import numpy as np

### This is the BUMBLEBEE output in a dataframe
# connect and query Teradata
con = teradatasql.connect(host='tdwd.group.on',user='ub_intl_sales_ops',password='BA_intl_grp_4a')

bumblebee = pd.read_sql('SELECT DISTINCT account_id,current_owner_id,employee_sf_id FROM sandbox.bumblebee_output where employee_sf_id IS NOT NULL',con)
bumblebee['account_id'] = bumblebee['account_id'].str.strip()

### This is the current account ownership in SF
account_list = bumblebee['account_id'].tolist()
account_list = "','".join(account_list)
login_sf = sf(username = 'intl_sales_ops_analysts@groupon.com', password = 'BA_intl_grp_3', security_token = 'ryDPBLSTkb54qbEY06GJSiA0')
soql_output = login_sf.bulk.Account.query(f"SELECT Account_ID_18__c,OwnerId,Acct_Owner_Change_Date__c FROM Account where Account_ID_18__c in ('{account_list}') limit 200000")

dic = {}
dic['sf_id'] = [ x['Account_ID_18__c'] for x in soql_output ]
dic['sf_OwnerId'] = [ x['OwnerId'] for x in soql_output ]
dic['sf_Acct_Owner_Change_Date__c'] = [ x['Acct_Owner_Change_Date__c'] for x in soql_output ]
    
current_owner_table = pd.DataFrame(dic, columns=dic.keys())

### This is current datetime
now = datetime.utcnow()
today = datetime.utcnow().strftime('%Y-%m-%d')

### process changes:
if not bumblebee.empty:
    ### The filtered table is ready for upload to SF
    # This joins the current_owner_table and the bumblebee table
    df = bumblebee.merge(current_owner_table, left_on='account_id', right_on='sf_id', how='left')

    # this filters the table on current_owner_id in Teradata compared to owner_id in SF, and also on the intended employee_sf_id to the current owner_id in SF
    filtered_table = df.drop(df[(df['current_owner_id'] != df['sf_OwnerId']) | (df['sf_OwnerId'] == df['employee_sf_id']) | (df['sf_Acct_Owner_Change_Date__c'] == today)].index)

    # this drops unnecesary columns
    filtered_table.drop('current_owner_id', axis=1, inplace=True)
    filtered_table.drop('sf_OwnerId', axis=1, inplace=True)
    filtered_table.drop('sf_Acct_Owner_Change_Date__c', axis=1, inplace=True)
    filtered_table.drop('sf_id', axis=1, inplace=True)
    
    # columns are renamed to fit Salesforce names
    filtered_table.columns = ['Id','OwnerId']

    print(filtered_table)
    
    ready_for_upload = []

    for row in filtered_table.itertuples():
        d = row._asdict()
        del d['Index']
        ready_for_upload.append(d)
        
    login_sf.bulk.Account.update(ready_for_upload,batch_size=10000,use_serial=True)
    
    for row in ready_for_upload:
        try:
            Id = row['Id']
            OwnerId = row['OwnerId']
            load_date = now
            query = f"INSERT INTO sandbox.bumblebee_processed_changes ('{Id}','{OwnerId}','{load_date}')"
            pd.read_sql_query(query,con)
        except:
            pass
        
    print('Processed changes: Success')
        
    ### Error conditions to check
    conditions = [
        (df['sf_OwnerId'] == df['employee_sf_id']),
        (df['current_owner_id'] != df['sf_OwnerId'])
        ]
    ### status to show in error table
    values = [
        'Account has already been moved to the intended owner',
        'Account-ownership is different in Teradata and Salesforce, indicating change has already been made'
        ]

    df['Status'] = np.select(conditions,values)

    ### prepare table for upload
    error_table = df.drop(df[(df['Status'] == '0')].index)
    error_table.drop('current_owner_id', axis=1, inplace=True)
    error_table.drop('employee_sf_id', axis=1, inplace=True)
    error_table.drop('sf_OwnerId', axis=1, inplace=True)

    errors_for_upload = []

    for row in error_table.itertuples():
        d = row._asdict()
        del d['Index']
        errors_for_upload.append(d)

    ### data is uploaded to Teradata
    for row in errors_for_upload:
        account_id = row['account_id']
        status = row['Status']
        load_date = now
        query = f"INSERT INTO sandbox.bumblebee_errors ('{account_id}','{status}','{load_date}')"
        pd.read_sql_query(query,con)
        
    print("Error upload: Success")

else:
    print("No data in Bumblebee")
    exit