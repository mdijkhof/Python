# last update on 2022-11-30

import pandas as pd
from simple_salesforce import bulk
from simple_salesforce import Salesforce as sf
import teradatasql
import math

def batch(iterable, n=1):
    l = len(iterable)
    for ndx in range(0, l, n):
        yield iterable[ndx:min(ndx + n, l)]


### This is the BUMBLEBEE output in a dataframe
# connect and query Teradata
con = teradatasql.connect(host='tdwd.group.on',user='ub_intl_sales_ops',password='BA_intl_grp_4a')

bumblebee = pd.read_sql('SELECT DISTINCT account_id,current_owner_id,employee_sf_id FROM sandbox.bumblebee_output where employee_sf_id IS NOT NULL',con)
bumblebee['account_id'] = bumblebee['account_id'].str.strip()

### This is the current account ownership in SF
account_list = bumblebee['account_id'].tolist()

batch_size = 2000
num_of_acc = len(account_list)
num_of_batches = math.ceil(len(account_list)/batch_size)
start_batch = 1

print(str(num_of_acc)+' accounts will be processed in '+str(num_of_batches)+' batches of '+str(batch_size))

for chunk in batch(account_list,batch_size):
    
    print("Batch "+str(start_batch)+" started:")
    
    acc_list = "','".join(chunk)
        
    login_sf = sf(username = 'intl_sales_ops_analysts@groupon.com', password = 'BA_intl_grp_3', security_token = 'ryDPBLSTkb54qbEY06GJSiA0')
    soql_output = login_sf.bulk.Account.query(f"SELECT Account_ID_18__c,OwnerId,Acct_Owner_Change_Date__c FROM Account where Account_ID_18__c in ('{acc_list}') limit 200000")

    dic = {}
    dic['sf_id'] = [ x['Account_ID_18__c'] for x in soql_output ]
    dic['sf_OwnerId'] = [ x['OwnerId'] for x in soql_output ]
    dic['sf_Acct_Owner_Change_Date__c'] = [ x['Acct_Owner_Change_Date__c'] for x in soql_output ]
        
    current_owner_table = pd.DataFrame(dic, columns=dic.keys())

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

        
        print(str(len(filtered_table))+' accounts to be processed in this batch')
        
        ready_for_upload = []

        for row in filtered_table.itertuples():
            d = row._asdict()
            del d['Index']
            ready_for_upload.append(d)
            
        login_sf.bulk.Account.update(ready_for_upload,batch_size=1000,use_serial=False)
        
        print("Batch "+str(start_batch)+" of "+str(num_of_batches)+" completed!")
        
        start_batch = start_batch+1
    
        
print("All batches completed!")