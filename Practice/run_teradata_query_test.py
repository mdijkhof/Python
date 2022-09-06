import teradatasql
import pandas as pd

con = teradatasql.connect(host='tdwd.group.on',user='mdijkhof',password='seinfeld66P^')
query = 'SELECT DISTINCT deal_uuid FROM sandbox.unagi WHERE report_date >= Current_Date-7 SAMPLE 50'

df = pd.read_sql(query,con)

# deal_uuid = []

# for deal in df:
#     deal_uuid.append(deal)

deal_uuid = df['deal_uuid'].tolist()

print(deal_uuid)
