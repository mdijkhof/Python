
from simple_salesforce import Salesforce
   
sf = Salesforce(username = 'svc_rao@groupon.com', password = 'Password-1Je8IR8frn0gkbS68Eq4OHZmM', security_token = '')

account_ids = ['001C000001T3FQrIAN','001C000001UTGRhIAP','001C000001UTGQKIA5']
owner_ids = ['005C0000003vg7TIAQ','005C0000003vg7TIAQ','005C0000003vg7TIAQ']


account_id = '001C000001T3FQrIAN'
owner_id = '005C0000003vg7TIAQ'

for account_id in account_ids:
    sf.Account.update(account_id,{'OwnerId':owner_id})