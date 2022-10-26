import pandas as pd
from simple_salesforce import Salesforce as sf
import teradatasql

login_sf = sf(username = 'intl_sales_ops_analysts@groupon.com', password = 'BA_intl_grp_3', security_token = 'ryDPBLSTkb54qbEY06GJSiA0')
con = teradatasql.connect(host='tdwd.group.on',user='ub_intl_sales_ops',password='BA_intl_grp_3')

### This is the data that needs to be processed, as set up in Teradata
pds_boost_data = pd.read_sql("SELECT account_id,pds_boost_flag,required_service,load_date FROM sandbox.pds_boost_account_flags WHERE load_date = (SELECT Max(load_date) FROM sandbox.pds_boost_account_flags)",con)

pds_boost_data['account_id'] = pds_boost_data['account_id'].str.strip()
td_account_ids = pds_boost_data['account_id'].tolist()
td_pds_boost_flags = pds_boost_data['pds_boost_flag'].tolist()
td_required_services = pds_boost_data['required_service'].tolist()
td_load_dates = pds_boost_data['load_date'].tolist()

data = {
    'td_account_id': td_account_ids,
    'td_pds_boost_flag': td_pds_boost_flags,
    'td_required_service': td_required_services,
    'td_load_date': td_load_dates    
}

td_pds_boost_data_table = pd.DataFrame(data)
print(td_pds_boost_data_table)

### This is the SF data table
soql_output = login_sf.query("SELECT Account_ID_18__c,PDS_Boost_Summary__c,PDS_Boost_Date__c,PDS_Boost__c FROM Account WHERE PDS_Boost_Summary__c != null")

sf_account_ids = []
sf_pds_boost_flags = []
sf_required_services = []
sf_load_dates = []

for row in soql_output['records']:
    sf_account_id = row['Account_ID_18__c']
    sf_account_ids.append(sf_account_id)
    sf_pds_boost_flag = row['PDS_Boost__c']
    sf_pds_boost_flags.append(sf_pds_boost_flag)
    sf_required_service = row['PDS_Boost_Summary__c']
    sf_required_services.append(sf_required_service)
    sf_load_date = row['PDS_Boost_Date__c']
    sf_load_dates.append(sf_load_date)

data = {
    'sf_account_id': sf_account_ids,
    'sf_pds_boost_flag': sf_pds_boost_flags,
    'sf_required_service': sf_required_services,
    'sf_load_date': sf_load_dates    
}

sf_pds_boost_data_table = pd.DataFrame(data)
print(sf_pds_boost_data_table)

### Data prep for the 'Delete Step' 
### Data from TD And SF is joined
df1 = sf_pds_boost_data_table.merge(td_pds_boost_data_table, left_on='sf_account_id', right_on='td_account_id', how='left')

# filter is being applied to only have accounts that need to be deleted
df1 = df1[(df1['td_account_id'].isnull())]

# this drops unnecesary columns
df1.drop('td_account_id', axis=1, inplace=True)
df1.drop('td_pds_boost_flag', axis=1, inplace=True)
df1.drop('sf_load_date', axis=1, inplace=True)
df1.drop('sf_required_service', axis=1, inplace=True)

# columns are renamed to fit Salesforce names
df1.columns = ['Account_ID_18__c','PDS_Boost__c','PDS_Boost_Summary__c','PDS_Boost_Date__c']

### Data prep for the 'Update Step'
### Data from TD And SF is joined
df = td_pds_boost_data_table.merge(sf_pds_boost_data_table,left_on='td_account_id', right_on='sf_account_id', how='left')

# filter is being applied to only have accounts that need to be deleted
df2 = df[((df['sf_required_service'] == df['td_required_service']) & (df['sf_load_date'] != df['td_load_date']))] # data where only a date change is needed
df3 = df[(df['sf_required_service'] != df['td_required_service'])] # data where a full change is needed

### Data prep for date change
# this drops unnecesary columns
df2.drop('sf_account_id', axis=1, inplace=True)
df2.drop('td_pds_boost_flag', axis=1, inplace=True)
df2.drop('sf_pds_boost_flag', axis=1, inplace=True)
df2.drop('sf_required_service', axis=1, inplace=True)
df2.drop('td_required_service', axis=1, inplace=True)
df2.drop('sf_load_date', axis=1, inplace=True)

df2['td_load_date'] = df2['td_load_date'].astype(str)

# columns are renamed to fit Salesforce names
df2.columns = ['Account_ID_18__c','PDS_Boost_Date__c']

### Data prep for full change
# this drops unnecesary columns
df3.drop('sf_account_id', axis=1, inplace=True)
df3.drop('td_pds_boost_flag', axis=1, inplace=True)
df3.drop('sf_load_date', axis=1, inplace=True)
df3.drop('sf_required_service', axis=1, inplace=True)

df3['td_load_date'] = df3['td_load_date'].astype(str)

# columns are renamed to fit Salesforce names
df3.columns = ['Account_ID_18__c','PDS_Boost_Summary__c','PDS_Boost_Date__c','PDS_Boost__c']

### data is prepared for upload in SF
df1_ready_for_upload = []

for row in df1.itertuples():
    d = row._asdict()
    del d['Index']
    df1_ready_for_upload.append(d)
    
### data is uploaded into salesforce
for row in df1_ready_for_upload:
    row['PDS_Boost_Summary__c'] = None
    row['PDS_Boost_Date__c'] = None
    row['PDS_Boost__c'] = 'false'
    login_sf.Account.update(row['Account_ID_18__c'],{'PDS_Boost__c':row['PDS_Boost__c'],'PDS_Boost_Summary__c':row['PDS_Boost_Summary__c'],'PDS_Boost_Date__c':row['PDS_Boost_Date__c']})

### data is prepared for upload in SF
df2_ready_for_upload = []

for row in df2.itertuples():
    d = row._asdict()
    del d['Index']
    df2_ready_for_upload.append(d)

### data is uploaded into salesforce
for row in df2_ready_for_upload:
      login_sf.Account.update(row['Account_ID_18__c'],{'PDS_Boost_Date__c':row['PDS_Boost_Date__c']})

### data is prepared for upload in SF
df3_ready_for_upload = []

for row in df3.itertuples():
    d = row._asdict()
    del d['Index']
    df3_ready_for_upload.append(d)

### data is uploaded into salesforce
for row in df3_ready_for_upload:
    row['PDS_Boost__c'] = 'true'
    login_sf.Account.update(row['Account_ID_18__c'],{'PDS_Boost__c':row['PDS_Boost__c'],'PDS_Boost_Summary__c':row['PDS_Boost_Summary__c'],'PDS_Boost_Date__c':row['PDS_Boost_Date__c']})

print('Success')