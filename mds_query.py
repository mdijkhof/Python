import requests
import json

# # connect to Teradata with a query to load in deal_uuids to check
# con = teradatasql.connect(host='tdwd.group.on',user='mdijkhof',password='seinfeld66P^')
# query = 'SELECT DISTINCT deal_uuid FROM sandbox.unagi WHERE report_date >= Current_Date-7 SAMPLE 50'

# # convert deal_uuids from query to list
# df = pd.read_sql(query,con)
# # deal_uuid = df['deal_uuid'].tolist()

deal_uuid = ['vegan-high-tea-de-haven-ster-huizen']

deal_state = []

""" loop over deal_uuids in list and find the status in deal catalog """
for deal in deal_uuid:
    url = f'https://mds.groupondev.com/deals/{deal}?client=lpo&show=all'
    response = requests.get(url)
    data = response.json()
    
    print(json.dumps(data,indent=4,sort_keys=False))