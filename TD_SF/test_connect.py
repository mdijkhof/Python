from connect import td_query as td
from connect import sf_query as sf

# td_query = td('SELECT DISTINCT deal_uuid FROM sandbox.unagi WHERE report_date >= Current_Date-7 SAMPLE 10')

# print(td_query)


sf_query = sf('SELECT Account_ID_18__c,OwnerId FROM Account where Account_ID_18__c','001C000001j1VQoIAM')

print(sf_query)