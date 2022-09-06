
import requests
import pandas as pd

deal_uuid = ['f1656539-5acb-470a-86da-20ab7842f1c0']
url = f'http://deal-catalog.snc1/deal_catalog/v2/deals/{deal_uuid}?clientId=f183e5fbee1bb4cd-display-ads'




# json file
response = requests.get(url)
data = response.json()

status = data['deal']['status']
print(status)







