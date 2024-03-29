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
login_sf = sf(username = 'intl_sales_ops_analysts@groupon.com', password = 'BA_intl_grp_3', security_token = 'ryDPBLSTkb54qbEY06GJSiA0')

# define variables
account_ids = bumblebee['account_id'].tolist()
owner_ids = []
change_dates = []

# get data from SOQL query and add to dictionary
for account_id in account_ids:
    soql_output = login_sf.query(f"SELECT Account_ID_18__c,OwnerId,Acct_Owner_Change_Date__c FROM Account where Account_ID_18__c = '{account_id}'")
    owner_id = soql_output['records'][0]['OwnerId']
    owner_ids.append(owner_id)
    change_date = soql_output['records'][0]['Acct_Owner_Change_Date__c']
    change_dates.append(change_date)
    
data = {
    'account_id': account_ids,
    'owner_id': owner_ids,
    'change_date': change_dates
}

current_owner_table = pd.DataFrame(data)

now = datetime.utcnow().strftime('%Y-%m-%d')

if not bumblebee.empty:
    ### The filtered table is ready for upload to SF
    # This joins the current_owner_table and the bumblebee table
    df = bumblebee.join(current_owner_table.set_index('account_id'), on='account_id')

    # this filters the table on current_owner_id in Teradata compared to owner_id in SF, and also on the intended employee_sf_id to the current owner_id in SF
    filtered_table = df.drop(df[(df['current_owner_id'] != df['owner_id']) | (df['owner_id'] == df['employee_sf_id']) | (df['change_date'] == now)].index)

    # this drops unnecesary columns
    filtered_table.drop('current_owner_id', axis=1, inplace=True)
    filtered_table.drop('owner_id', axis=1, inplace=True)
    filtered_table.drop('change_date', axis=1, inplace=True)

    # columns are renamed to fit Salesforce names
    filtered_table.columns = ['Id','OwnerId']

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
            load_date = load_date = str(date.today())
            query = f"INSERT INTO sandbox.bumblebee_processed_changes ('{Id}','{OwnerId}','{load_date}')"
            pd.read_sql_query(query,con)
        except:
            pass

    print('Processed changes: Success')

    ### Error conditions to check
    conditions = [
        (df['owner_id'] == df['employee_sf_id']),
        (df['current_owner_id'] != df['owner_id'])
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
    error_table.drop('owner_id', axis=1, inplace=True)

    errors_for_upload = []

    for row in error_table.itertuples():
        d = row._asdict()
        del d['Index']
        errors_for_upload.append(d)

    ### data is uploaded to Teradata
    for row in errors_for_upload:
        account_id = row['account_id']
        status = row['Status']
        load_date = str(date.today())
        query = f"INSERT INTO sandbox.bumblebee_errors ('{account_id}','{status}','{load_date}')"
        
        pd.read_sql_query(query,con)
        
    print("Error upload: Success")

else:
    print("No data in Bumblebee")
    exit