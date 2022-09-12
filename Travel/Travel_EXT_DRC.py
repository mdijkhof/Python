import os
import requests
import json
import pygsheets
from datetime import datetime

now = datetime.utcnow().strftime('%Y-%m-%d')

gc = pygsheets.authorize(service_file=os.path.expanduser("~")+"\\.megatron\\google_service_account_secret.json")

sh = gc.open_by_key('1IdFaTUqxf6v6V_GONlVsNWbobajlPEPRZ-XyDm-gUAQ')
wks_divisions = sh.worksheet_by_title("active_divisions")
wks_deals = sh.worksheet_by_title("live_deals")
live_deals = wks_deals.get_values("A1","B10000")
divisions = [["load_date","deal_uuid","contract_id","country_code"]]
errors = []

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
            country = c
            divisions.append([now,deal_uuid,contract_id,country])
    except:
        errors.append(deal_uuid)
        pass

crange = f'A1:D{len(divisions)}'
wks_divisions.clear()
wks_divisions.update_values(crange, divisions)