def td_query(sql_query):
    import teradatasql
    import pandas as pd

    con = teradatasql.connect(host='tdwd.group.on',user='mdijkhof',password='seinfeld66P&')
    output = pd.read_sql(sql_query,con)
    
    return output



def sf_query(soql_query,input_id):
    from simple_salesforce import Salesforce as sf
    
    login_sf = sf(username = 'intl_sales_ops_analysts@groupon.com', password = 'BA_intl_grp_3', security_token = 'ryDPBLSTkb54qbEY06GJSiA0')
    output = login_sf.query(f"{soql_query} = '{input_id}'")
    
    return output



def sf_login():
    from simple_salesforce import Salesforce as sf
        
    sf_login = sf(username = 'svc_rao@groupon.com', password = 'Password-1Je8IR8frn0gkbS68Eq4OHZmM', security_token = '')
    
    return sf_login