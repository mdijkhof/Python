def planningLeadtime(sfdc,id,startDate,leadtime,StartDate_adj):
    ''' Check Vette Date to keep leadtime for deal scheduling
        Compare with the start date given in the request
        Returns:
            date in string format 'yyyy-mm-dd'
    '''

    # pull information about opportunity/account/opportunity owner from salesforce
    soql_output = sfdc.query(f"SELECT Opportunity.Deal_Vette_Time__c, Opportunity.Category_v3__c, Opportunity.Subcategory_v3__c, Opportunity.Primary_Deal_Services__c, Opportunity.City_Planning_Private_Notes__c, Account.id, Account.Type, Account.Feature_country__c, Opportunity.Owner.id, Opportunity.Owner.Name, Opportunity.Owner.Revenue_Center__c FROM Opportunity WHERE id ='{id}'")

    vetteDate = soql_output['records'][0]['Deal_Vette_Time__c']
    category_v3 = soql_output['records'][0]['Category_v3__c']
    subcategory_v3 = soql_output['records'][0]['Subcategory_v3__c']
    pds = soql_output['records'][0]['Primary_Deal_Services__c']
    national_approval_notes = soql_output['records'][0]['City_Planning_Private_Notes__c'] or ''
    
    account_type = soql_output['records'][0]['Account']['Type']
    feature_country = soql_output['records'][0]['Account']['Feature_Country__c']
    
    owner_revenue_center = soql_output['records'][0]['Owner']['Revenue_Center__c']

    # leadtime in days
    # check if Core National Merchants 
    if (feature_country not in ['AU']
        and owner_revenue_center in ['National Sales Team', 'Groupon Live']
        and account_type not in ['Live']
        and category_v3 not in ['Tickets']
        and subcategory_v3 not in ['Courses - eLearning', 'Courses - Continuing Education']
        and not any(pds_string in pds for pds_string in ['Custom -', 'eLearning', 'Photo Printing', 'Scrapbooking / Personal Calendar', 'Photo Processing',  'Custom Printing', '- Custom', '- Online Custom' ])
        ):
        # based on marketing input re: package, different lead times could be applied (2022-07-29) - https://docs.google.com/presentation/d/1PrWM4sBF7DjUm1brFngC5tyudHdcIhF7y2SwUpHxLUQ/edit#slide=id.g13e8fbb3ca1_0_0
            if 'package 1' in national_approval_notes.lower():
                leadtime = 10
            elif 'package 2' in national_approval_notes.lower():
                leadtime = 10
            elif 'package 3' in national_approval_notes.lower():
                leadtime = 5
            else:
                leadtime = 2

    # else apply standard leadtime
    else:
        leadtime = 2
        return leadtime

    if startDate == '':
        output = startDate
        return output

    try:
        vetteDate = datetime.strptime(vetteDate.split('T')[0], "%Y-%m-%d").date()
        startDate_adj = max(datetime.strptime(startDate, "%Y-%m-%d").date(), vetteDate + timedelta(days=leadtime))
        startDate_adj = startDate_adj.strftime('%Y-%m-%d')
        output = startDate_adj
        return StartDate_adj

    except Exception as e:
        output = startDate

    return output