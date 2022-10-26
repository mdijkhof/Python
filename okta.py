import requests
from requests_auth import Basic
import json

r = requests.get('http://deal-catalog.snc1/deal_catalog/v2/deals/eed82d1e-9ffc-4950-9fad-2c1bda5295cb?clientId=f183e5fbee1bb4cd-display-ads', auth=Basic('mdijkhof', 'seinfeld66P&'))
#r = requests.get('https://deal-catalog.production.service.us-west-1.aws.groupondev.com/deal_catalog/v2/deals/eed82d1e-9ffc-49[…]?clientId=f183e5fbee1bb4cd-display-ads', auth=Basic('mdijkhof', 'seinfeld66P&'))
#r = requests.get('https://groupon.okta.com/', auth=Basic('mdijkhof', 'seinfeld66P&'))
#r = requests.get('https://deal-catalog.production.service.us-west-1.aws.groupondev.com', auth=Basic('mdijkhof', 'seinfeld66P&'))
#r = requests.get('http://deal-catalog.snc1', auth=Basic('mdijkhof', 'seinfeld66P&'))

json_deal_catalogue = json.loads(r.text)            
print(json_deal_catalogue)