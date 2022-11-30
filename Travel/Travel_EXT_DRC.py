import requests
import json
from datetime import datetime
import teradatasql
import pandas as pd

### Connect to Teradata and get all deals that need to be checked in Deal Catalogue
con = teradatasql.connect(host='tdwd.group.on',user='mdijkhof',password='seinfeld66P&')
input_data = pd.read_sql("SELECT deal_uuid,contract_number FROM sandbox.EMEA_Travel_deals_dim WHERE deal_ends_at >= Current_Date AND booking_voucher = 'Booking' AND country_name IN ('BE','DE','ES','FR','GB', 'IE', 'IT', 'NL', 'PL','AU') GROUP BY 1,2",con)

live_deals = input_data.values.tolist()

print(live_deals)

### Set up additional variables
now = datetime.utcnow().strftime('%Y-%m-%d')

### For all deal_uuids, check the deal_catalog values and add them to the Divisions variable and insert data into teradata
for i in range(1,len(live_deals)):
    deal_uuid = live_deals[i][0]
    contract_id = live_deals[i][1]

    url = f'http://deal-catalog.snc1/deal_catalog/v2/deals/{deal_uuid}?clientId=f183e5fbee1bb4cd-display-ads'
    try:
        r = requests.get(url)
        json_deal_catalogue = json.loads(r.text)
        DRC = []
        DRC = json_deal_catalogue["deal"]["distributionRegionCodes"]
        for c in DRC:
            query = f"INSERT INTO sandbox.getaways_xnt_drc ('{now}','{deal_uuid}','{contract_id}','{c}')"
            pd.read_sql_query(query,con)
    except:
        pass
  
print("Success")