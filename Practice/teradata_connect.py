def td_query(sql_query):
    import teradatasql
    import pandas as pd

    con = teradatasql.connect(host='tdwd.group.on',user='mdijkhof',password='seinfeld66P&')
    output = pd.read_sql(sql_query,con)
    
    return output



def sf_query(soql_query,input_id):
    import simple_salesforce as sf
    
    soql_query = sf.Salesforce(username = 'svc_rao@groupon.com', password = 'Password-1Je8IR8frn0gkbS68Eq4OHZmM', security_token = '')
    output = sf.query(f"{soql_query} = '{input_id}'")
    
    return output