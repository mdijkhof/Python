def generate_case_description(sfdc,opp_id,email_address,request_type,country,opportunity_link,account_id,division,start_date,end_date,new_start_date,new_end_date,side_PR_start_date,side_PR_end_date,new_expiration_date,request_type_attribute,add_remove_attribute,attribute_name,comments,pre_verification_comments):
    """
    ========== DOCSTRING ==========
    Purpose: generate content for Description field for salesforce Case
    return string with request details as an output
    ========== ***end*** ==========
    """

    try:
        nl = '\n' # new line tag to use in f-string expression

        # pull information about opportunity/account/opportunity owner from salesforce
        soql_output = sfdc.query(f"SELECT Opportunity.Deal_Vette_Time__c, Opportunity.Category_v3__c, Opportunity.Subcategory_v3__c, Opportunity.Primary_Deal_Services__c, Opportunity.City_Planning_Private_Notes__c, Account.id, Account.Type, Account.Feature_country__c, Opportunity.Owner.id, Opportunity.Owner.Name, Opportunity.Owner.Revenue_Center__c FROM Opportunity WHERE id ='{opp_id}'")

        category_v3 = soql_output['records'][0]['Category_v3__c']
        subcategory_v3 = soql_output['records'][0]['Subcategory_v3__c']
        pds = soql_output['records'][0]['Primary_Deal_Services__c']
        national_approval_notes = soql_output['records'][0]['City_Planning_Private_Notes__c'] or ''
        vetteDate = soql_output['records'][0]['Deal_Vette_Time__c']
        account_type = soql_output['records'][0]['Account']['Type']
        feature_country = soql_output['records'][0]['Account']['Feature_Country__c']
        
        owner_revenue_center = soql_output['records'][0]['Owner']['Revenue_Center__c']

        # check if Core National Merchants 
        if (request_type in ['Move scheduled deal']
            and owner_revenue_center in ['National Sales Team', 'Groupon Live']):
                if (feature_country not in ['AU']
                    and account_type not in ['Live']
                    and category_v3 not in ['Tickets']
                    and subcategory_v3 not in ['Courses - eLearning', 'Courses - Continuing Education']
                    and not any(pds_string in pds for pds_string in ['Custom -', 'eLearning', 'Photo Printing', 'Scrapbooking / Personal Calendar', 'Photo Processing','Custom Printing', '- Custom', '- Online Custom'])
                ):
                    # leadtime in days
                        if ['package 1','package 2'] in national_approval_notes.lower():
                            leadtime = 10
                        elif 'package 3' in national_approval_notes.lower():
                            leadtime = 5
                        else:
                            leadtime = 2
                            
                        if start_date == '':
                            output = start_date
                            return output
                    
                    
                        # define vetteDate and startDate_adj in order to determine if comment needs to be added later on
                        vetteDate = datetime.strptime(vetteDate.split('T')[0], "%Y-%m-%d").date()
                        startDate_adj = max(datetime.strptime(start_date, "%Y-%m-%d").date(), vetteDate + timedelta(days=leadtime))
                        startDate_adj = startDate_adj.strftime('%Y-%m-%d')
                   
                    

                 
                #     core_national_comment = "Manager's approval needed"
                # else:
                #     core_national_comment = "Manager's approval not needed"
        else:
            core_national_comment = ''

        output = f"Requestor: {email_address}{nl}"

        output += f"Request type: {request_type}{nl}" \
            f"{'Core National comment: ' + core_national_comment + nl if core_national_comment !='' else ''}" \
            f"{'country: ' + country + nl if country !='' else ''}" \
            f"{'Opportunity id: ' + opportunity_link + nl if opportunity_link !='' else ''}" \
            f"{'Account id: ' + account_id + nl if account_id !='' else ''}" \
            f"{'Division: ' + division + nl if division !='' else ''}" \
            f"{'Start Date: ' + start_date + nl if start_date !='' else ''}" \
            f"{'End Date: ' + end_date + nl if end_date !='' else ''}" \
            f"{'New Start Date: ' + new_start_date + nl if new_start_date !='' else ''}" \
            f"{'New End Date: ' + new_end_date + nl if new_end_date !='' else ''}" \
            f"{'Side PR Start Date: ' + side_PR_start_date + nl if side_PR_start_date !='' else ''}" \
            f"{'Side PR End Date: ' + side_PR_end_date + nl if side_PR_end_date !='' else ''}" \
            f"{'New Expiration Date: ' + new_expiration_date + nl if new_expiration_date !='' else ''}" \
            f"{'Attribute request type: ' + request_type_attribute + nl if request_type_attribute !='' else ''}" \
            f"{'Attribute action: ' + add_remove_attribute + nl if add_remove_attribute !='' else ''}" \
            f"{'Attribute Name: ' + attribute_name + nl if attribute_name !='' else ''}" \
            f"{'Comments: ' + comments + nl if comments !='' else ''}" \
            f"{'Pre-verification comments: ' + pre_verification_comments + nl if pre_verification_comments !='' else ''}"

    except:
        output = 'error'

    return output