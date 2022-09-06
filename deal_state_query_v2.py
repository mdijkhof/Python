import requests
import pandas as pd
from datetime import date
import teradatasql
from megatron import saving as m
import json

# connect to Teradata with a query to load in deal_uuids to check
con = teradatasql.connect(host='tdwd.group.on',user='mdijkhof',password='seinfeld66P^')
query = "SELECT DISTINCT opp2.deal_uuid FROM dwh_load_sf_view.sf_opportunity_1 AS opp JOIN dwh_load_sf_view.sf_opportunity_2 AS opp2 ON 1=1 AND opp.id = opp2.id WHERE 1=1 AND opp.feature_country = 'GB' AND opp.category_v3 NOT IN ('Travel','Goods') AND opp.closedate >= '2022-01-01' AND deal_state IS NOT NULL"


# convert deal_uuids from query to list
df = pd.read_sql(query,con)
deal_uuid = df['deal_uuid'].tolist()

deal_state = []

# loop over deal_uuids in list and find the status in deal catalog
for deal in deal_uuid:
    url = f'http://deal-catalog.snc1/deal_catalog/v2/deals/{deal}?clientId=f183e5fbee1bb4cd-display-ads'
    response = requests.get(url)
    data = response.json()
    
    # print(json.dumps(data,indent=4,sort_keys=False))
    
    status = data['deal']['status']
    deal_state.append(status)

# create a dataset with 3 columns
data = {
    'deal_uuid': deal_uuid, 
    'deal_state': deal_state, 
    'load_date': str(date.today())
    }

# push data to Teradata table
data_table = pd.DataFrame(data)
m.df_to_sql(data_table,table_name='md_deal_state_check',  adapter='teradata')

print("Success")
