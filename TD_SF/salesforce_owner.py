import pandas as pd
from megatron import saving as m
from connect import td_query as td
from connect import sf_query as sf

# define variables
owner_ids = []

# connect and query Teradata
query = td('SELECT DISTINCT account_id,current_owner_id,employee_sf_id FROM sandbox.bumblebee_output')
print(query) # this is a dataframe 

account_ids = query['account_id'].tolist()


# get data from SOQL query ad add to dictionary
for account_id in account_ids:
    soql_output = sf('SELECT Account_ID_18__c,OwnerId FROM Account where Account_ID_18__c',account_id)
    owner_id = soql_output['records'][0]['OwnerId']
    owner_ids.append(owner_id)

data = {
    'account_id': account_ids,
    'owner_id': owner_ids
    } 

# upload data to teradata
data_table = pd.DataFrame(data)

print(data_table)


# m.df_to_sql(data_table,table_name='bumblebee_owner_check',  adapter='teradata')

# print("Success")