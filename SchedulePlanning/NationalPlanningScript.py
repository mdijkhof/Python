"""
============= DOCSTRING ==========
Purpose:
handle requests submitted to Case Approval Team. Create planning records in salesforce
for divisions and feature dates specified by requestor, and/or create a salesforce Case for other
types of requests
Process Map:
https://lucid.app/documents/view/9d774001-3ee8-4876-bdfa-b6723de6a522
============= ***end*** ==========
"""

from megatron import salesforce as sf
from megatron import saving as save
from megatron import functions as f
from megatron import mailing as m

import string
from numpy.core.numeric import NaN
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

# ============= INIT =============
output = list()
output.append(f.timestamp('start', f.project_name))

gdoc_link = '11HA_UtdoZKIA3ZLRYuPYc27drhovMI-PnKJ_TTI-nXU'
input_sheet = 'requests'

#adding a flag to check job result
status_flag = True

division_map = {
        'BE': {
            'West-Vlaanderen': 'a0PC000000EB7yJMAT',
            'Oost-Vlaanderen': 'a0PC000000EB7yIMAT',
            'Antwerpen': 'a0PC000000EB7XIMA1',
            'Vlaams-Brabant': 'a0PC000000EB7yCMAT',
            'Limburg BE': 'a0PC000000EB7xYMAT',
            'Bruxelles': 'a0PC000000EB7XNMA1',
            'Liège': 'a0PC000000EB7FaMAL',
            'Namur': 'a0PC000000EB7XfMAL',
            'Charleroi': 'a0PC000000EB7XOMA1',
            'Mons - Tournai': 'a0PC000000EB7XdMAL'},

        'DE': {
            'Aachen': 'a0PC000000EB7bYMAT',
            'Augsburg': 'a0PC000000EB7bZMAT',
            'Berlin': 'a0PC000000EB7baMAD',
            'Bielefeld': 'a0PC000000EB7bbMAD',
            'Bochum': 'a0PC000000EB7bcMAD',
            'Bonn': 'a0PC000000EB7bdMAD',
            'Braunschweig': 'a0PC000000EB7beMAD',
            'Bremen': 'a0PC000000EB7bfMAD',
            'Dortmund': 'a0PC000000EB7biMAD',
            'Dresden': 'a0PC000000EB7bjMAD',
            'Duisburg/Oberhausen': 'a0PC000000EB7bkMAD',
            'Düsseldorf': 'a0PC000000EB7blMAD',
            'Essen': 'a0PC000000EB7bpMAD',
            'Frankfurt': 'a0PC000000EB7bqMAD',
            'Hamburg': 'a0PC000000EB7bvMAD',
            'Hannover': 'a0PC000000EB7bwMAD',
            'Karlsruhe': 'a0PC000000EB7bzMAD',
            'Kiel': 'a0PC000000EB7c1MAD',
            'Koblenz': 'a0PC000000EB7c2MAD',
            'Köln': 'a0PC000000EB7c3MAD',
            'Leipzig/Halle': 'a0PC000000EB7c4MAD',
            'Lübeck': 'a0PC000000EB7c5MAD',
            'Magdeburg': 'a0PC000000EB7c6MAD',
            'Mainz': 'a0PC000000EB7c7MAD',
            'Mannheim': 'a0PC000000EB7c8MAD',
            'München': 'a0PC000000EB7c9MAD',
            'Münster': 'a0PC000000EB7cAMAT',
            'Nürnberg': 'a0PC000000EB7cCMAT',
            'Osnabrück': 'a0PC000000EB7cHMAT',
            'Rostock': 'a0PC000000EB7cNMAT',
            'Stuttgart': 'a0PC000000EB7cRMAT',
            'Ulm': 'a0PC000000EB7cTMAT',
            'Wiesbaden': 'a0PC000000EB7cUMAT',
            'Würzburg': 'a0PC000000EB7cWMAT'},

        'ES': {
            'Donostia': 'a0PC000000EB7ePMAT',
            'A Coruña': 'a0PC000000EB7dQMAT',
            'Bilbao': 'a0PC000000EB7deMAD',
            'Alicante': 'a0PC000000EB7dWMAT',
            'Almería': 'a0PC000000EB7dXMAT',
            'Asturias': 'a0PC000000EB7eGMAT',
            'Badajoz': 'a0PC000000EB7daMAD',
            'Barcelona': 'a0PC000000EB7dcMAD',
            'Cadiz': 'a0PC000000EB7diMAD',
            'Girona': 'a0PC000000EB7dsMAD',
            'Granada': 'a0PC000000EB7dvMAD',
            'Las Palmas': 'a0PC000000EB7e2MAD',
            'Madrid': 'a0PC000000EB7e8MAD',
            'Málaga': 'a0PC000000EB7eAMAT',
            'Valencia': 'a0PC000000EB7eeMAD',
            'Valladolid': 'a0PC000000EB7egMAD',
            'Vigo': 'a0PC000000EB7eiMAD',
            'Vitoria (ES)': 'a0PC000000EB7dRMAT',
            'Sevilla': 'a0PC000000EB7eVMAT',
            'Santander': 'a0PC000000EB7eTMAT',
            'Sant. de Compostela': 'a0PC000000EB7FoMAL',
            'Salamanca': 'a0PC000000EB7eOMAT',
            'Córdoba': 'a0PC000000EB7dnMAD',
            'Pamplona': 'a0PC000000EB7eJMAT',
            'Palma de Mallorca': 'a0PC000000EB7eIMAT',
            'Murcia': 'a0PC000000EB7eEMAT',
            'Zaragoza': 'a0PC000000EB7emMAD',
            'Tarragona': 'a0PC000000EB7eXMAT',
            'Tenerife': 'a0PC000000EB7SOMA1'},

        'FR': {
            'Aix-en-Provence': 'a0PC000000EB7fPMAT',
            'Amiens': 'a0PC000000EB7fSMAT',
            'Angers': 'a0PC000000EB7fTMAT',
            'Annecy': 'a0PC000000EB7fVMAT',
            'Antibes-Cannes': 'a0PC000000EB7fXMAT',
            'Avignon': 'a0PC000000EB7fZMAT',
            'Belfort - Montbeliard': 'a0PC000000EB7FBMA1',
            'Bordeaux': 'a0PC000000EB7fhMAD',
            'Boulogne - Issy': 'a0PC000000EB7RnMAL',
            'Brest - Quimper': 'a0PC000000EB7fjMAD',
            'Béziers - Narbonne': 'a0PC000000EB7feMAD',
            'Caen': 'a0PC000000EB7fkMAD',
            'Cergy - Pontoise': 'a0PC000000EB7RoMAL',
            'Chambéry': 'a0PC000000EB7foMAD',
            'Chelles - Marne La Vallée': 'a0PC000000EBfgyMAD',
            'Clermont-Ferrand': 'a0PC000000EB7fsMAD',
            'Colmar': 'a0PC000000EB7ftMAD',
            'Dijon - Beaune': 'a0PC000000EB7fuMAD',
            'Essonne': 'a0PC000000EB7fyMAD',
            'Evry - Corbeil': 'a0PC000000EB7RuMAL',
            'Fréjus - Saint-Raphaël': 'a0PC000000EB7IaMAL',
            'Grenoble': 'a0PC000000EB7g0MAD',
            'Hauts-de-Seine': 'a0PC000000EB7g2MAD',
            'La Baule-Saint Nazaire': 'a0PC000000EB7gjMAD',
            'La Défense - Nanterre': 'a0PC000000EB7RwMAL',
            'La Rochelle': 'a0PC000000EB7g4MAD',
            'Le Havre': 'a0PC000000EB7g6MAD',
            'Lens-Douai-Arras': 'a0PC000000EB7JIMA1',
            'Le Mans': 'a0PC000000EB7g7MAD',
            'Lille': 'a0PC000000EB7g8MAD',
            'Limoges': 'a0PC000000EB7g9MAD',
            'Lorient - Vannes': 'a0PC000000EB7gAMAT',
            'Lyon': 'a0PC000000EB7gBMAT',
            'Marseille': 'a0PC000000EB7gDMAT',
            'Metz': 'a0PC000000EB7gGMAT',
            'Montpellier': 'a0PC000000EB7gIMAT',
            'Mulhouse': 'a0PC000000EB7gJMAT',
            'Nancy': 'a0PC000000EB7gKMAT',
            'Nantes': 'a0PC000000EB7gLMAT',
            'Nice': 'a0PC000000EB7gPMAT',
            'Nîmes - Alès': 'a0PC000000EB7gRMAT',
            'Oise': 'a0PC000000EB7gTMAT',
            'Orléans': 'a0PC000000EB7gVMAT',
            'Paris': 'a0PC000000EB7gWMAT',
            'Paris 12e - 15e': 'a0PC000000EB7JlMAL',
            'Paris 1er - 6e': 'a0PC000000EB7JhMAL',
            'Paris 7e 8e 16e 17e': 'a0PC000000EB7JkMAL',
            'Paris 9e - 11e 18e - 20e': 'a0PC000000EB7JjMAL',
            'Paris extra': 'a0PC000000EB7FwMAL',
            'Pays Basque': 'a0PC000000EB7fbMAD',
            'Pays Bearnais': 'a0PC000000EB7gXMAT',
            'Perpignan': 'a0PC000000EB7gZMAT',
            'Poitiers': 'a0PC000000EB7gbMAD',
            'Reims': 'a0PC000000EB7gdMAD',
            'Rennes': 'a0PC000000EB7geMAD',
            'Rouen': 'a0PC000000EB7ggMAD',
            'Saint Germain-en-Laye': 'a0PC000000EB7S2MAL',
            'Saint Maur - Champigny': 'a0PC000000EB7S3MAL',
            'Saint-Etienne': 'a0PC000000EB7ghMAD',
            'Savigny - Sainte-Geneviève': 'a0PC000000EB7S5MAL',
            'Seine et Marne': 'a0PC000000EB7glMAD',
            'Strasbourg': 'a0PC000000EB7gnMAD',
            'Toulon': 'a0PC000000EB7grMAD',
            'Toulouse': 'a0PC000000EB7gsMAD',
            'Tours': 'a0PC000000EB7guMAD',
            'Val-d-Oise': 'a0PC000000EB7gxMAD',
            'Val-de-Marne': 'a0PC000000EB7gwMAD',
            'Valence - Montelimar': 'a0PC000000EB7gyMAD',
            'Valenciennes': 'a0PC000000EB7gzMAD',
            'Versailles': 'a0PC000000EB7h2MAD',
            'Yvelines': 'a0PC000000EB7h3MAD'},

        'AU': {
            'Sydney': 'a0PC000000EB7xAMAT',
            'Melbourne': 'a0PC000000EB7x6MAD',
            'Perth': 'a0PC000000EB7x8MAD',
            'Adelaide': 'a0PC000000EB7wuMAD',
            'Brisbane': 'a0PC000000EB7wxMAD',
            'Gold Coast': 'a0PC000000EB7x2MAD',
            'Canberra': 'a0PC000000EB7wzMAD',
            'Newcastle (AU)': 'a0PC000000EB7LmMAL',
            'Wollongong': 'a0PC000000EB7xBMAT'},

        'IE': {
            'Dublin': 'a0PC000000EB7hXMAT',
            'Galway': 'a0PC000000EB7haMAD',
            'Munster': 'a0PC000000EB7hWMAT'},

        'IT': {
            'Ancona e Marche': 'a0PC000000EB7iTMAT',
            'Bari': 'a0PC000000EB7iXMAT',
            'Reggio Emilia': 'a0PC000000EB7jjMAD',
            'Bergamo': 'a0PC000000EB7iaMAD',
            'Bologna e Ferrara': 'a0PC000000EB7ibMAD',
            'Brescia': 'a0PC000000EB7ieMAD',
            'Cagliari e Sardegna': 'a0PC000000EB7igMAD',
            'Calabria': 'a0PC000000EB7jiMAD',
            'Catania e Sicilia Orientale': 'a0PC000000EB7ikMAD',
            'Como e Lecco': 'a0PC000000EB7ioMAD',
            'Firenze ed entroterra': 'a0PC000000EB7ivMAD',
            'Genova e Liguria': 'a0PC000000EB7j0MAD',
            'Lecce e Salento': 'a0PC000000EB7j8MAD',
            'Litorale Toscano': 'a0PC000000EB7jXMAT',
            'Milano': 'a0PC000000EB7jGMAT',
            'Modena': 'a0PC000000EB7jJMAT',
            'Monza Brianza': 'a0PC000000EB7jKMAT',
            'Napoli': 'a0PC000000EB7jLMAT',
            'Novara': 'a0PC000000EB7jNMAT',
            'Padova': 'a0PC000000EB7jPMAT',
            'Palermo': 'a0PC000000EB7jQMAT',
            'Parma e Piacenza': 'a0PC000000EB7jRMAT',
            'Perugia e Umbria': 'a0PC000000EB7jTMAT',
            'Pescara e Abruzzo': 'a0PC000000EB7jVMAT',
            'Ravenna': 'a0PC000000EB7jgMAD',
            'Rimini e Pesaro': 'a0PC000000EB7jkMAD',
            'Roma e Lazio': 'a0PC000000EB7jlMAD',
            'Salerno': 'a0PC000000EB7jrMAD',
            'Taranto': 'a0PC000000EB7jzMAD',
            'Torino e Piemonte': 'a0PC000000EB7k1MAD',
            'Trentino-Alto Adige': 'a0PC000000EB7k5MAD',
            'Treviso': 'a0PC000000EB7k6MAD',
            'Trieste': 'a0PC000000EB7k7MAD',
            'Udine e Pordenone': 'a0PC000000EB7k8MAD',
            'Varese': 'a0PC000000EB7k9MAD',
            'Venezia Mestre': 'a0PC000000EB7kAMAT',
            'Verona': 'a0PC000000EB7kBMAT',
            'Vicenza': 'a0PC000000EB7kDMAT'},

        'NL': {
            'Den Bosch': 'a0PC000000EB7neMAD',
            'Nijmegen': 'a0PC000000EB7o5MAD',
            'Arnhem': 'a0PC000000EB7nZMAT',
            'Utrecht': 'a0PC000000EB7oCMAT',
            'Het Gooi': 'a0PC000000EB7nvMAD',
            'Friesland': 'a0PC000000EB7nyMAD',
            'Groningen': 'a0PC000000EB7noMAD',
            'Drenthe': 'a0PC000000EB7naMAD',
            'Zwolle': 'a0PC000000EB7oLMAT',
            'Twente': 'a0PC000000EB7nmMAD',
            'Amsterdam': 'a0PC000000EB7nXMAT',
            'Apeldoorn-Deventer': 'a0PC000000EB7nYMAT',
            'Alkmaar': 'a0PC000000EB7nSMAT',
            'Haarlem': 'a0PC000000EB7nrMAD',
            'Den Haag': 'a0PC000000EB7GGMA1',
            'Leiden': 'a0PC000000EB7nzMAD',
            'Breda': 'a0PC000000EB7ncMAD',
            'Eindhoven': 'a0PC000000EB7nkMAD',
            'Rotterdam': 'a0PC000000EB7o8MAD',
            'Tilburg': 'a0PC000000EB7oBMAT',
            'Almere': 'a0PC000000EB7nTMAT',
            'Zeeland': 'a0PC000000EB7H0MAL',
            'Limburg': 'a0PC000000EBxGLMA1'},

        'PL': {
            'Warszawa': 'a0PC000000EB7q9MAD',
            'Trójmiasto': 'a0PC000000EB7q6MAD',
            'Szczecin': 'a0PC000000EB7q3MAD',
            'Olsztyn': 'a0PC000000EB7ptMAD',
            'Bydgoszcz': 'a0PC000000EB7paMAD',
            'Toruń': 'a0PC000000EB7q5MAD',
            'Wrocław': 'a0PC000000EB7qHMAT',
            'Poznań': 'a0PC000000EB7pwMAD',
            'Opole': 'a0PC000000EB7puMAD',
            'Radom': 'a0PC000000EB7pyMAD',
            'Lublin': 'a0PC000000EB7psMAD',
            'Białystok': 'a0PC000000EB7pYMAT',
            'Rzeszów': 'a0PC000000EB7q1MAD',
            'Łódź': 'a0PC000000EB7prMAD',
            'Kielce': 'a0PC000000EB7pnMAD',
            'Kraków': 'a0PC000000EB7ppMAD',
            'Katowice': 'a0PC000000EB7pmMAD',
            'Częstochowa': 'a0PC000000EB7pdMAD',
            'Gliwice': 'a0PC000000EB7phMAD',
            'Rybnik': 'a0PC000000EB7q0MAD',
            'Sosnowiec': 'a0PC000000EB7q2MAD',
            'Tychy': 'a0PC000000EB7q7MAD',
            'Bielsko-Biała': 'a0PC000000EB7pZMAT'},

        'UAE': {
            'Abu Dhabi': 'a0PC000000EB7WLMA1',
            'Dubai': 'a0PC000000EB7WMMA1',
            'Sharjah and N. Emirates': 'a0PC000000EB7WPMA1'},

        'UK': {
            'Aberdeen': 'a0PC000000EB7urMAD',
            'Belfast': 'a0PC000000EB7uvMAD',
            'Birmingham (UK)': 'a0PC000000EB7uxMAD',
            'Bournemouth': 'a0PC000000EB7v1MAD',
            'Brighton': 'a0PC000000EB7v3MAD',
            'Bristol & Bath': 'a0PC000000EB7v4MAD',
            'Cambridgeshire': 'a0PC000000EB7v6MAD',
            'Cardiff & South Wales': 'a0PC000000EB7v7MAD',
            'Chester / North Wales': 'a0PC000000EB7vAMAT',
            'Coventry & Warks': 'a0PC000000EB7vCMAT',
            'Derby': 'a0PC000000EB7vEMAT',
            'Dundee': 'a0PC000000EB7vHMAT',
            'Edinburgh': 'a0PC000000EB7vJMAT',
            'Essex': 'a0PC000000EB7vLMAT',
            'Exeter': 'a0PC000000EB7vOMAT',
            'Falkirk & Stirling': 'a0PC000000EB7vPMAT',
            'Glasgow': 'a0PC000000EB7vQMAT',
            'Gloucestershire': 'a0PC000000EB7vRMAT',
            'Hertfordshire': 'a0PC000000EB7wKMAT',
            'Hull': 'a0PC000000EB7vUMAT',
            'Kent': 'a0PC000000EB7vXMAT',
            'Lancashire': 'a0PC000000EB7vZMAT',
            'Leeds & Bradford': 'a0PC000000EB7vaMAD',
            'Leicester': 'a0PC000000EB7vbMAD',
            'Liverpool': 'a0PC000000EB7vdMAD',
            'London': 'a0PC000000EB7veMAD',
            'London East': 'a0PC000000EB7vfMAD',
            'London North': 'a0PC000000EB7vgMAD',
            'London South': 'a0PC000000EB7vhMAD',
            'London Special': 'a0PC000000EB7JOMA1',
            'London West': 'a0PC000000EB7viMAD',
            'Manchester': 'a0PC000000EB7vlMAD',
            'N.Bucks & Bedfordshire': 'a0PC000000EB7vmMAD',
            'Newcastle (UK)': 'a0PC000000EB7voMAD',
            'Norfolk': 'a0PC000000EB7vrMAD',
            'Northamptonshire': 'a0PC000000EB7vqMAD',
            'Nottingham & Lincolnshire': 'a0PC000000EB7vsMAD',
            'Oxford': 'a0PC000000EB7vvMAD',
            'Plymouth': 'a0PC000000EB7vxMAD',
            'Portsmouth': 'a0PC000000EB7vzMAD',
            'Reading': 'a0PC000000EB7w2MAD',
            'Sheffield': 'a0PC000000EB7w5MAD',
            'Southampton': 'a0PC000000EB7w8MAD',
            'Suffolk': 'a0PC000000EB7vVMAT',
            'Surrey': 'a0PC000000EB7wEMAT',
            'Teesside': 'a0PC000000EB7wHMAT',
            'West Midlands & Staffordshire': 'a0PC000000EB7wLMAT',
            'Wiltshire': 'a0PC000000EB7TsMAL',
            'Worcestershire': 'a0PC000000EB7wNMAT',
            'York & Harrogate': 'a0PC000000EB7wQMAT'}
    }

# ============= CORE =============

def salesforce_id_converter(id):
    ''' Convert 15-char salesfoce id into 18-character id
        Credits: https://github.com/amureki/salesforce_id_converter/blob/master/salesforce_id_converter/converter.py
        Returns:
            18-char id or error
    '''
    if len(id)==15:
        suffix = ''
        # split id into three chunks
        chunks = [id[i:i+5] for i in range(0, len(id), 5)]
        # construct an array of A-Z and 0-5
        array = string.ascii_uppercase + string.digits[:6]

        for chunk in chunks:
            bit_char = str(int(''.join(reversed(['1' if str(char).isupper() else '0' for char in chunk])), 2))
            suffix += array[int(bit_char)]

        return id + suffix
    elif len(id)==18:
        return id
    else:
        return 'error'

def pre_verification(sfdc,id):
    ''' Check if an opportunity is vetted or Groupon Expiration Date on opportunity page is not in the past or a deal in not on A Sure Thing already
        Requests with violations will be rejected
        Returns:
            string with rejection reason
    '''

    output = ''
    try:
        sf_output = sfdc.query(f"SELECT Deal_Strengh__c, Deal_Vette_Time__c, Groupon_Expiration_Date__c FROM Opportunity WHERE id ='{id}'")

        dealStrength = sf_output['records'][0]['Deal_Strengh__c']
        vetteDate = sf_output['records'][0]['Deal_Vette_Time__c']
        expirationDate = sf_output['records'][0]['Groupon_Expiration_Date__c']
        expirationDate = datetime.strptime(expirationDate, "%Y-%m-%d").date() if expirationDate is not None else ''

        if dealStrength == 'A Sure Thing':
            output+= '<br>- Opportunity is on A Sure Thing already. If you want to add the new planning records select "Add an extra PR" in the request form. If you want to change a start date/end date of your deal, please select "Move Scheduled Deal".'

        if expirationDate!='':
            if expirationDate <= datetime.today().date():
                output+= '<br>- Groupon Expiration Date on opportunity page is in the past.'

        if vetteDate is None:
            output+= '<br>- Deal is not vetted. Please fill in the form again once your opportunity is vetted.'

    except Exception as a:
        output = '[ERROR] request pre verification failed'

    return output

def planningLeadtime(sfdc,id,startDate):
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
                leadtime = int(10)
            elif 'package 2' in national_approval_notes.lower():
                leadtime = int(10)
            elif 'package 3' in national_approval_notes.lower():
                leadtime = int(5)
            else:
                leadtime = int(2)

    # else apply standard leadtime
    else:
        leadtime = int(2)
        
    

    if startDate == '':
        output = startDate
        return output

    try:
        vetteDate = datetime.strptime(vetteDate.split('T')[0], "%Y-%m-%d").date()
        startDate_adj = max(datetime.strptime(startDate, "%Y-%m-%d").date(), vetteDate + timedelta(days=leadtime))
        startDate_adj = startDate_adj.strftime('%Y-%m-%d')
        output = startDate_adj
        
    except Exception as e:
        output = startDate

    return output,leadtime
    
def planningEndDate(sfdc,id,startDate,endDate):
    ''' Review end date if not earlier than the adjusted start date (leadtime rule driven by planningLeadtime function),
        as well as to make sure it's before the Groupon Expiration Date
        Check Groupon Expiration Date on opportunity level
        Compare it with the end date given in the request. If requested endDate is greater or equal Groupon Expiration Date, set endDate = Groupon_Expiration-1
        Returns:
            date in string format 'yyyy-mm-dd'
    '''

    if endDate == '':
        output = endDate
        return output

    try:
        expirationDate = sfdc.query(f"SELECT Groupon_Expiration_Date__c FROM Opportunity WHERE id ='{id}'")['records'][0]['Groupon_Expiration_Date__c']

        # convert sfdc response into date format
        expirationDate = datetime.strptime(expirationDate, "%Y-%m-%d").date() if expirationDate is not None else ''

        if expirationDate == '': # expiration date is empty
            endDate_adj = endDate
        elif datetime.strptime(endDate, "%Y-%m-%d").date() >= expirationDate: # end date is greater or equal expiration
            endDate_adj = (expirationDate - timedelta(days=1)).strftime('%Y-%m-%d')
        else:
            endDate_adj = endDate

        if startDate > endDate_adj:
            endDate_adj = (datetime.strptime(startDate, "%Y-%m-%d").date() + timedelta(days=1)).strftime('%Y-%m-%d')

        output = endDate_adj

    except Exception as e:
        output = endDate

    return output

def confirm_request_completion(recipient,opp_id,country,division,start_date,end_date):
    """
    ========== DOCSTRING ==========
    Purpose: send an email to requestor with confrimation
    return status as an output
    ========== ***end*** ==========
    """

    try:
        recipients_list = [recipient]
        subject = f'Local deal scheduling - request completed'
        body_parts_list = f"<p>Your request for deal scheduling has been completed</p>" \
                        f"<p><ul>" \
                        f"<li>Opportunity id: {opp_id}</li>" \
                        f"<li>Country: {country}</li>" \
                        f"<li>Division: {division}</li>" \
                        f"<li>Start Date: {start_date}</li>" \
                        f"<li>End Date: {end_date}</li>" \
                        f"</ul></p>"
        body_parts_list += f"<i>This is automatically generated email. Contact cat@groupon.com if further support is needed.</i>"

        m.send_email(recipients=recipients_list, subjectline= subject, body_parts = [body_parts_list], no_reply=True, how='smtp')
        output = 'email_sent'

    except:
        output = 'error'

    return output

def reject_requests_email(recipient,opp_id,country,division,start_date,end_date,rejection_reason):
    """
    ========== DOCSTRING ==========
    Purpose: send an email to requestor with confrimation
    return status as an output
    ========== ***end*** ==========
    """
    try:
        recipients_list = [recipient]
        subject = f'Local deal scheduling - request rejected'
        body_parts_list = f"<p>Your 'Plan a deal' request for deal scheduling <b>could not be completed</b>. Please resolve the issues and send the new requests through the form.</p>" \
                        f"<p>Reason:{rejection_reason}</p>" \
                        f"<p><ul>" \
                        f"<li>Opportunity id: {opp_id}</li>" \
                        f"<li>Country: {country}</li>" \
                        f"<li>Division: {division}</li>" \
                        f"<li>Start Date: {start_date}</li>" \
                        f"<li>End Date: {end_date}</li>" \
                        f"</ul></p>"
        body_parts_list += f"<i>This is automatically generated email. Contact cat@groupon.com if further support is needed.</i>"

        m.send_email(recipients=recipients_list, subjectline= subject, body_parts = [body_parts_list], no_reply=True, how='smtp')
        output = 'rejection_sent'

    except:
        output = 'error'

    return output

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
        deal_vette_time = soql_output['records'][0]['Deal_Vette_Time__c']
        account_type = soql_output['records'][0]['Account']['Type']
        feature_country = soql_output['records'][0]['Account']['Feature_Country__c']
        startDate = planningLeadtime[0]
        leadtime = planningLeadtime[1]
        owner_revenue_center = soql_output['records'][0]['Owner']['Revenue_Center__c']
        
        # convert deal_vette_time in order to compare to start date later on
        deal_vette_time = datetime.strptime(deal_vette_time.split('T')[0], "%Y-%m-%d").date()
        deal_vette_time_adj = (deal_vette_time + timedelta(days=leadtime))
        deal_vette_time_adj = deal_vette_time.strftime('%Y-%m-%d')

        # leadtime in days
        # check if Core National Merchants 
        if (request_type in ['Move scheduled deal']
            and owner_revenue_center in ['National Sales Team', 'Groupon Live']):
                if (feature_country not in ['AU']
                    and account_type not in ['Live']
                    and category_v3 not in ['Tickets']
                    and subcategory_v3 not in ['Courses - eLearning', 'Courses - Continuing Education']
                    and not any(pds_string in pds for pds_string in ['Custom -', 'eLearning', 'Photo Printing', 'Scrapbooking / Personal Calendar', 'Photo Processing','Custom Printing', '- Custom', '- Online Custom'])
                ):
                    if startDate >= deal_vette_time_adj:
                        core_national_comment = 'Ok to reschedule'
                    else:
                        core_national_comment = national_approval_notes + ': check the vette time'
                
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

def main() -> list:
    """All-in-one function to attach planning records to opportunities and set deal strength to 'A Sure Thing'.
    Credits:
        https://github.com/simple-salesforce/simple-salesforce#using-bulk
    Returns:
        list: log to be sent via `email_log` function
    """
    global status_flag

    output = list()

    form_responses = save.gdoc_to_df(gdoc= gdoc_link,sheet_name=input_sheet, remove_unnamed= True, evaluate_formulas=True).fillna('')

    #merge columns for input
    # NOTE: columns names in the source gdoc must correspond with the names below
    # This section needs to be updated when new qestions are added in the requests submission form
    # Relevant changes may be necessary in the further sections in such a case

    input = pd.DataFrame()
    input['request_timestamp'] = form_responses['Timestamp']
    input['email_address'] = form_responses['Email Address']
    input['request_type'] = form_responses['Please select type of the request:']
    input['opportunity_link'] = form_responses.loc[:, form_responses.columns.str.startswith('Please provide a link to opportunity:')].apply(lambda x: "".join(x.astype(str)), axis=1)
    input['country'] = form_responses.loc[:, form_responses.columns.str.startswith('Please select country')].apply(lambda x: "".join(x.astype(str)), axis=1)
    input['account_id'] = form_responses.loc[:, form_responses.columns.str.startswith('Please provide account ID')].apply(lambda x: "".join(x.astype(str)), axis=1)
    input['division'] = form_responses.loc[:, form_responses.columns.str.startswith('Please select divisions')].apply(lambda x: "".join(x.astype(str)), axis=1)
    input['start_date'] = form_responses.loc[:, form_responses.columns.str.startswith('Please select feature date')].apply(lambda x: "".join(x), axis=1)
    input['start_date'] = input['start_date'].apply(lambda x: '' if x=='' else datetime.strptime(x, "%m/%d/%Y").date().strftime('%Y-%m-%d'))
    input['end_date'] = form_responses.loc[:, form_responses.columns.str.startswith('Please select end date')].apply(lambda x: "".join(x), axis=1)
    input['end_date'] = input['end_date'].apply(lambda x: '' if x=='' else datetime.strptime(x, "%m/%d/%Y").date().strftime('%Y-%m-%d'))
    input['new_start_date'] = form_responses.loc[:, form_responses.columns.str.startswith('Please select new feature date')].apply(lambda x: "".join(x.astype(str)), axis=1)
    input['new_start_date'] = input['new_start_date'].apply(lambda x: '' if x=='' else datetime.strptime(x, "%m/%d/%Y").date().strftime('%Y-%m-%d'))
    input['new_end_date'] = form_responses.loc[:, form_responses.columns.str.startswith('Please select new end date')].apply(lambda x: "".join(x.astype(str)), axis=1)
    input['new_end_date'] = input['new_end_date'].apply(lambda x: '' if x=='' else datetime.strptime(x, "%m/%d/%Y").date().strftime('%Y-%m-%d'))
    input['side_PR_start_date'] = form_responses.loc[:, form_responses.columns.str.startswith('Please select side PR feature date')].apply(lambda x: "".join(x.astype(str)), axis=1)
    input['side_PR_start_date'] = input['side_PR_start_date'].apply(lambda x: '' if x=='' else datetime.strptime(x, "%m/%d/%Y").date().strftime('%Y-%m-%d'))
    input['side_PR_end_date'] = form_responses.loc[:, form_responses.columns.str.startswith('Please select side PR end date')].apply(lambda x: "".join(x.astype(str)), axis=1)
    input['side_PR_end_date'] = input['side_PR_end_date'].apply(lambda x: '' if x=='' else datetime.strptime(x, "%m/%d/%Y").date().strftime('%Y-%m-%d'))
    input['new_expiration_date'] = form_responses.loc[:, form_responses.columns.str.startswith('Please select new Groupon expiration date')].apply(lambda x: "".join(x.astype(str)), axis=1)
    input['new_expiration_date'] = input['new_expiration_date'].apply(lambda x: '' if x=='' else datetime.strptime(x, "%m/%d/%Y").date().strftime('%Y-%m-%d'))
    input['request_type_attribute'] = form_responses.loc[:, form_responses.columns.str.startswith('Please choose type of your request for attribute update:')].apply(lambda x: "".join(x.astype(str)), axis=1)
    input['add_remove_attribute'] = form_responses.loc[:, form_responses.columns.str.startswith('Please specify if you want to add or remove an attribute:')].apply(lambda x: "".join(x.astype(str)), axis=1)
    input['attribute_name'] = form_responses.loc[:, form_responses.columns.str.startswith('Please select the attribute:')].apply(lambda x: "".join(x.astype(str)), axis=1)
    input['comments'] = form_responses.loc[:, form_responses.columns.str.startswith('Comments')].apply(lambda x: "".join(x.astype(str)), axis=1)

    # extract id from opportunity link
    try:
        input['opp_id_temp'] = input.apply(lambda x: x['opportunity_link'].replace('/view','').split("/")[-1].split("?")[0], axis = 1)
        input['opp_id'] = input.apply(lambda x: salesforce_id_converter(x['opp_id_temp']),axis=1)
        input.drop(columns = ['opp_id_temp'], axis=1, inplace=True)
    except Exception as e:
        output.append(f.timestamp('error', details=f'extracting id from opportunity link failed! message: {e}'))
        status_flag = False
        return output

    # generate request id as a concatenation of request timestamp and opp_id in a format: YYYYmd_HMS_oppid
    try:
        input['request_id'] = input.apply(lambda x: f"{''.join(map(str,datetime.strptime(x['request_timestamp'], '%m/%d/%Y %H:%M:%S').timetuple()[0:3]))}_{''.join(map(str,datetime.strptime(x['request_timestamp'], '%m/%d/%Y %H:%M:%S').timetuple()[3:6]))}_{x['opp_id']}", axis=1)
    except Exception as e:
        output.append(f.timestamp('error', details=f'generating request_id failed! message: {e}'))
        status_flag = False
        return output

    # pull completed requests from the log and proceed with pending requests
    try:
        curation_table = save.gdoc_to_df(gdoc= gdoc_link,sheet_name='logs_after', remove_unnamed= True, evaluate_formulas=True).fillna('')
        curation_table = curation_table.query("status != 'error'") 
        input = input[~input['request_id'].isin(curation_table['request_id'].values.tolist())]
    except Exception as e:
        output.append(f.timestamp('error', details=f'getting the curation table failed! message: {e}'))
        status_flag = False
        return output

    if len(input) == 0:
        return output

    output.append(f.timestamp('info', details=f'{len(input.index)} requests to be processed'))

    # open saleforce session
    f.timestamp('info', details=f'attempting to open a Salesforce session')
    try:
        sfdc = sf.open_session()
        f.timestamp('info', details='opening Salesforce session succeeded!')
    except Exception as e:
        output.append(f.timestamp('error', details=f'opening Salesforce session failed! message: {e}'))
        status_flag = False
        return output

    # preverification of requests
    try:
        input['rejection_reason'] = input.apply(lambda x: pre_verification(sfdc,x['opp_id']) if x['request_type'] == 'Plan a deal' else '', axis=1)
    except Exception as e:
        output.append(f.timestamp('error', details=f'pre-verification failed!: {e}'))
        status_flag = False

    # review start date to keep leadtime in planning since vette date
    input['start_date'] = input.apply(lambda x: planningLeadtime(sfdc,x['opp_id'],x['start_date']) if x['request_type'] == 'Plan a deal' and x['rejection_reason'] == '' else x['start_date'], axis=1)
    # review end date if not earlier than the adjusted start date (leadtime rule)
    input['end_date'] = input.apply(lambda x: planningEndDate(sfdc,x['opp_id'],x['start_date'],x['end_date']) if x['request_type'] == 'Plan a deal' and x['rejection_reason'] == '' else x['end_date'], axis=1)

    ### PLAN A DEAL ###

    pending_schedule = input.query("request_type == 'Plan a deal' & rejection_reason == ''")
    post_ds = pd.DataFrame()

    if len(pending_schedule) == 0:
        f.timestamp('info', details='No opportunities to schedule')

    else:
        # replace "All divisions" selection with the actual comma-separated division names
        try:
            pending_schedule['division'] = pending_schedule.apply(lambda x: x['division'] if 'All divisions' not in x['division'] else ', '.join(division_map[x['country']]), axis = 1)
        except Exception as e:
            output.append(f.timestamp('error', details=f'replacing "All divisions" with a list of all division names failed! message: {e}'))
            status_flag = False
            return output

        # explode columns with comma-separated divisions into separate rows
        # credits: https://medium.com/@sureshssarda/pandas-splitting-exploding-a-column-into-multiple-rows-b1b1d59ea12e
        df_schedule_deals = pd.DataFrame()
        try:
            df_schedule_deals = pd.DataFrame(pd.DataFrame(pending_schedule.division.str.split(',').tolist(), index=[pending_schedule.country,pending_schedule.opp_id,pending_schedule.start_date,pending_schedule.end_date]).stack())
            df_schedule_deals.reset_index(inplace=True)
            df_schedule_deals.columns = ['country','Opportunity__c','Start_Date__c','End_Date__c','level','Division_name']
        except Exception as e:
            output.append(f.timestamp('error', details=f'exploding rows into individual divisions failed! message: {e}'))
            status_flag = False
            return output

        # map division names with id
        try:
            df_schedule_deals['Division__c'] = df_schedule_deals.apply(lambda x: division_map[x['country']][x['Division_name'].strip()], axis = 1)
        except Exception as e:
            output.append(f.timestamp('error', details=f'mapping division names with division id failed! message: {e}'))
            status_flag = False
            return output

        df_schedule_deals.drop(columns=['country','level','Division_name'], inplace=True)

        df_schedule_deals['Position__c'] = 'Side'
        df_schedule_deals['Priority__c'] = '1'

        output.append(f.timestamp('info', details=f'{len(df_schedule_deals.index)} planning records to be created on {len(pending_schedule)} deal(s)'))

        # Mass Upload using bulk
        # Credits: https://github.groupondev.com/intl-xlob-analytics/GRPN_jobs/blob/master/TRAVEL-sf_planning_creation.py

        f.timestamp('info', details='inserting planning records and updating deal strength')
        pr_api_feedback = []
        df_pr_api_feedback = pd.DataFrame(columns=['pr_success','pr_created','pr_id','pr_errors'])
        ds_api_feedback = []
        df_ds_api_feedback = pd.DataFrame(columns=['ds_success','ds_created','ds_id','ds_errors'])

        try:
            # create planning records
            batches = f.chunkify(df_schedule_deals.to_dict(orient='records'), limit=200)

            for batch in batches:
                # if API returns empty message then this code fills the missing
                # rows with placeholder dicts this is to avoid shifting upwards the
                # entire table in case of shorter api response than input not
                # needed for deal strengths, because those are left joined rather
                # than appended
                api_feedback = sfdc.bulk.Planning__c.insert(batch)
                if len(api_feedback) < len(batch):
                    for i in range(len(batch)-len(api_feedback)):
                        api_feedback.append({'success': None, 'created': None, 'id': None, 'errors': [{'message': 'API call did not return any message'}]})
                pr_api_feedback.extend(api_feedback)
            df_pr_api_feedback = pd.DataFrame(pr_api_feedback)
            df_pr_api_feedback.rename(columns={'created': 'pr_created', 'errors': 'pr_errors', 'id': 'pr_id', 'success': 'pr_success'}, inplace=True)
            df_pr_api_feedback['pr_errors'] = df_pr_api_feedback['pr_errors'].apply(lambda x: None if str(x).strip().lower() in ['nan', '', '[]'] else str(x))
            f.timestamp('info', details=f'planning record insertion API call completed! \n{df_pr_api_feedback}')

            # merge per-division input with api feedback
            df_pr_api_feedback_consolidated = pd.merge(df_schedule_deals, df_pr_api_feedback, left_index=True, right_index=True)
            df_pr_api_feedback_consolidated['pr_is_error'] = df_pr_api_feedback_consolidated['pr_success'].apply(lambda x: 1 if x not in [True,'True'] else 0)

            # summarize on opportunity level to check if any PR failed
            df_opp_pr_api_feedback = pd.pivot_table(df_pr_api_feedback_consolidated, values='pr_is_error', index=['Opportunity__c'], aggfunc=[np.sum,'count']).reset_index()
            df_opp_pr_api_feedback.columns = ['Opportunity__c', 'pr_error_count', 'pr_count']

            # merge request-level pending_schedule dataframe with PR creation error count
            post_schedule = pending_schedule.join(df_opp_pr_api_feedback.set_index('Opportunity__c'), on='opp_id')

            f.timestamp('info', details=f'API feedback consolidation for Post Schedule dataframe completed')

            # setting A Sure Thing
            df_deal_strength = pd.DataFrame()
            df_deal_strength['Id'] = post_schedule.opp_id
            df_deal_strength['Deal_Strengh__c'] = 'A Sure Thing'
            
            '''
            # set deal strength only for opportunities with successfully created planning records
            batches = f.chunkify(df_deal_strength[post_schedule['pr_error_count'] == 0].to_dict(orient='records'), limit=200)
            if len(batches) != 0:
                for batch in batches:
                    if len(batch) > 0:  # sending empty batch would cause a Malformed request error
                        ds_api_feedback.extend(sf.bulk_opportunity_update(batch, sfdc, returns='list_of_dict'))
                df_ds_api_feedback = pd.DataFrame(ds_api_feedback)
                df_ds_api_feedback.rename(columns={'created': 'ds_created', 'errors': 'ds_errors', 'id': 'ds_id', 'success': 'ds_success'}, inplace=True)
            else:
                # if batch for Deal Strength is empty, create a dataframe for processing further
                df_ds_api_feedback = pd.DataFrame({'ds_created':[],'ds_errors':[],'ds_id':[],'ds_success':[]})
            df_ds_api_feedback['ds_errors'] = df_ds_api_feedback['ds_errors'].apply(lambda x: None if str(x).strip().lower() in ['nan', '', '[]'] else str(x))
            '''

            # set deal strength only for opportunities with successfully created planning records
            deals_for_AST = df_deal_strength[post_schedule['pr_error_count'] == 0]
            if len(deals_for_AST) != 0:
                ds_api_feedback = list()
                for index, row in deals_for_AST.iterrows():
                    ds_id = row[0]
                    sf_api_feedback = sfdc.Opportunity.update(ds_id, {'Deal_Strengh__c': 'A Sure Thing'})
                    if sf_api_feedback != 204:
                        errors = 'NOT_PROCESSED'
                        success = False
                    else:
                        errors = None
                        success = True
                    ds_api_feedback.append([False, errors, ds_id, success])

                df_ds_api_feedback = pd.DataFrame(ds_api_feedback)
                df_ds_api_feedback.columns = ['ds_created', 'ds_errors', 'ds_id', 'ds_success']
            else:
                # if batch for Deal Strength is empty, create a dataframe for processing further
                df_ds_api_feedback = pd.DataFrame({'ds_created':[],'ds_errors':[],'ds_id':[],'ds_success':[]})
            
            df_ds_api_feedback['ds_errors'] = df_ds_api_feedback['ds_errors'].apply(lambda x: None if str(x).strip().lower() in ['nan', '', '[]'] else str(x))

            f.timestamp('info', details=f'deal strength update API call completed! \n{df_ds_api_feedback}')

            # merge post_schedule with deal_strength update api feedback
            post_ds = post_schedule.join(df_ds_api_feedback[['ds_id','ds_success','ds_errors']].set_index('ds_id'), on='opp_id')

            f.timestamp('info', details=f'API feedback consolidation for Deal Strength status completed')

        except Exception as e:
            output.append(f.timestamp('error', details=f'salesforce api insert/update failed! message: {e}'))
            status_flag = False
            if len(df_pr_api_feedback)>0:
                pr_api_feedback_html = df_pr_api_feedback.to_html().replace('\n','')
                output.append(f.timestamp('info', details=f'Planning Record creation API feedback: \n {pr_api_feedback_html}'))
            if len(df_ds_api_feedback)>0:
                ds_api_feedback_html = df_ds_api_feedback.to_html().replace('\n','')
                output.append(f.timestamp('info', details=f'Deal Strength update API feedback: \n {ds_api_feedback_html}'))

        # send email to requestor about completion (only for successful)
        if len(post_ds)>0:
            f.timestamp('info', details='sending confirmation emails to requestors')
            try:
                post_ds['requestor_confirmation_email'] = post_ds.apply(lambda x: confirm_request_completion(x['email_address'],x['opp_id'],x['country'],x['division'],x['start_date'],x['end_date']) if x['ds_success'] in [True,'True'] else np.nan, axis=1)
            except Exception as e:
                output.append(f.timestamp('error', details=f'sending confirmation emails to requestors failed!: {e}'))
                status_flag = False
                return output

    ### CASE CREATION ###

    # merge input with results from deal planning section
    if len(post_ds)>0:
        input = pd.merge(input,post_ds[['request_timestamp','opportunity_link','pr_error_count','pr_count','ds_success','ds_errors','requestor_confirmation_email']].set_index(['request_timestamp','opportunity_link']), how='left', left_on=['request_timestamp','opportunity_link'], right_on=['request_timestamp','opportunity_link'])
    else:
        # add columns to input dataframe with null valies if post_ds is empty
        input['pr_error_count'] = np.nan
        input['pr_count'] = np.nan
        input['ds_success'] = np.nan
        input['ds_errors'] = np.nan
        input['requestor_confirmation_email'] = np.nan

    # send email to requestor in case a request is rejected
    try:
        input['requestor_confirmation_email'] = input.apply(lambda x: reject_requests_email(x['email_address'],x['opp_id'],x['country'],x['division'],x['start_date'],x['end_date'],x['rejection_reason']) if x['rejection_reason'] != '' and x['rejection_reason'] != '[ERROR] request pre verification failed' else x['requestor_confirmation_email'], axis=1)
    except Exception as e:
        output.append(f.timestamp('error', details=f'sending rejection emails to requestors failed!: {e}'))
        status_flag = False

    # pull the list of requests to have a case created
    df_handle_by_case = input.query("request_type != 'Plan a deal' | (requestor_confirmation_email != 'email_sent' & requestor_confirmation_email !='rejection_sent')")
    output.append(f.timestamp('info', details=f'{len(df_handle_by_case.index)} cases to be created'))
    if len(df_handle_by_case)==0:
        input['case_id'] = np.nan
        input['case_success'] = np.nan
        input['case_errors'] = np.nan

    else:
        # prepare api input file for bulk insert
        try:
            df_handle_by_case['Subject'] = df_handle_by_case.apply(lambda x: f"{'[' + x['country'] + '] ' if x['country']!='' else ''}{x['request_type']}", axis=1)
            df_handle_by_case['Description'] = df_handle_by_case.apply(lambda x: generate_case_description(sfdc,x['opp_id'],x['email_address'],x['request_type'],x['country'],x['opportunity_link'],x['account_id'],x['division'],x['start_date'],x['end_date'],x['new_start_date'],x['new_end_date'],x['side_PR_start_date'],x['side_PR_end_date'],x['new_expiration_date'],x['request_type_attribute'],x['add_remove_attribute'],x['attribute_name'],x['comments'],x['rejection_reason']), axis=1)
            df_handle_by_case['SuppliedEmail'] = df_handle_by_case.apply(lambda x: x['email_address'], axis=1)
            df_handle_by_case['Opportunity__c'] = df_handle_by_case.apply(lambda x: x['opp_id'] if x['opp_id'] != 'error' else '', axis=1)
            df_handle_by_case['RecordTypeId'] = '012C00000007pkLIAQ'
            df_handle_by_case['Origin'] = 'Deal Scheduling Support'
            df_handle_by_case['Status'] = 'New'
            df_handle_by_case['Support_Category__c'] ='Goods Deal Support'
            df_handle_by_case['Support_SubCategory__c'] = 'CSX Relaunch'
            df_handle_by_case['OwnerId'] = '00G3c000003r2yp' # Deal Scheduling

        except Exception as e:
            output.append(f.timestamp('error', details=f'Case creation api input failed!: {e}'))
            status_flag = False
            return output

        # mass create Cases in salesforce
        try:
            batch = df_handle_by_case[['Subject','Description','SuppliedEmail','Opportunity__c','RecordTypeId','Origin','Status','Support_Category__c','Support_SubCategory__c','OwnerId']].to_dict(orient='records')

            sf_api_feedback_case = []
            sf_api_feedback_case.extend(sfdc.bulk.Case.insert(batch))
            sf_api_feedback_case = pd.DataFrame(sf_api_feedback_case)[['id','success','errors']]
            sf_api_feedback_case.columns = ['case_id','case_success','case_errors']
            sf_api_feedback_case['case_errors'] = sf_api_feedback_case['case_errors'].apply(lambda x: None if str(x).strip().lower() in ['nan', '', '[]'] else str(x))
            
            f.timestamp('info', details=f'Case creation API feedback:\n{sf_api_feedback_case}')

        except Exception as e:
            output.append(f.timestamp('error', details=f'case creation failed!: {e}'))
            status_flag = False
            return output

        # merge df_handle_by_case with case creation api feedback
        df_handle_by_case = pd.merge(df_handle_by_case.reset_index(drop=True), sf_api_feedback_case, how='left', left_index=True, right_index=True)

        # merge input with case creation feedback
        input = pd.merge(input,df_handle_by_case[['request_timestamp','opportunity_link','case_id','case_success','case_errors']].set_index(['request_timestamp','opportunity_link']), how='left', left_on=['request_timestamp','opportunity_link'], right_on=['request_timestamp','opportunity_link'])

    # add status column summarizing request handling result
    # 'request_completed' if request was directly and successfully resolved by the script
    # 'case_created' if case successfully created for a request eligible for a manual handling or escalation
    # 'error' for anything else
    input['status'] = input.apply(lambda x: 'request_rejected' if x['requestor_confirmation_email'] == 'rejection_sent' else ('request_completed' if x['requestor_confirmation_email'] == 'email_sent' else ('case_created' if x['case_success'] in [True,'True'] else 'error')), axis=1)

    # save statuses to Teradata to keep history of checks
    processed_requests = pd.DataFrame(input[['request_id','request_timestamp','email_address','request_type','opportunity_link','country','opp_id','pr_error_count','pr_count','ds_success','ds_errors','requestor_confirmation_email','case_id','case_success','case_errors','rejection_reason','status']])

    try:
        dwh_created_at = f.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
        processed_requests['dwh_created_at'] = pd.to_datetime(dwh_created_at, errors='ignore')
        td_output = save.df_to_sql(processed_requests, 'sb_rmaprod.ao_deal_scheduling_log', how='append', public=True)
        output.extend(td_output)
        if not f.has_no_errors(td_output):  # dump dataframe to excel also if there were errors saving to Teradata
            output.extend(save.to_dump_file(processed_requests, object_name='processed_requests'))

    except Exception as e:
        output.append(f.timestamp('error', details=f'saving statuses to Teradata failed! message: {e}'))
        status_flag = False
        output.extend(save.to_dump_file(processed_requests, object_name='processed_requests'))

    # save logs into gdoc
    try:
        output.extend(save.df_to_gdoc(
            pd.concat([save.gdoc_to_df(gdoc_link, sheet_name='logs_after').query("dwh_created_at > '" + (f.datetime.utcnow() - timedelta(days=8)).strftime('%Y-%m-%d') + "'"), processed_requests], sort=False),
            gdoc_link,
            sheet_name='logs_after',
            how='replace'))
        f.timestamp('info', details='logs saved to gdoc!')
    except Exception as e:
        output.append(f.timestamp('error', details=f'saving logs to gdoc failed! message: {e}'))
        status_flag = False

    full_log = f"""
    select
        *
    from sb_rmaprod.ao_deal_scheduling_log
    where 1=1
        and dwh_created_at = '{dwh_created_at}'
    --
    """

    error_log = f"""
    select
        *
    from sb_rmaprod.ao_deal_scheduling_log
    where 1=1
        and dwh_created_at = '{dwh_created_at}'
        and status__c in ('error')
    --
    """

    # append additional metadata to output log
    count_rejected = processed_requests['request_id'][(processed_requests['status'].map(str.lower) == 'request_rejected')].count()
    count_scheduling_completed = processed_requests['request_id'][(processed_requests['status'].map(str.lower) == 'request_completed')].count()
    count_cases_completed = processed_requests['request_id'][(processed_requests['status'].map(str.lower) == 'case_created')].count()
    count_errors = processed_requests['request_id'][(processed_requests['status'].map(str.lower) == 'error')].count()

    output.append(f.timestamp('info', details=f'{len(processed_requests)} requests processed'))
    output.append(f.timestamp('info', details=f'{count_scheduling_completed}/{len(pending_schedule)} requests completed successfully for deal scheduling'))
    output.append(f.timestamp('info', details=f'{count_cases_completed} salesforce cases successfully created'))
    output.append(f.timestamp('info', details=f'{count_rejected} requests rejected'))
    if count_errors > 0:
        output.append(f.timestamp('error', details=f'{count_errors} requests failed'))
        status_flag = False
        html_table_errors = processed_requests.query("status == 'error'").to_html().replace('\n','')
        output.append(f.timestamp('info', details= f'Logs:\n {html_table_errors}'))

    if count_errors > 0:
        output.append(f.timestamp('info', details=f'check error details here:\n{error_log}\n'))
    output.append(f.timestamp('info', details=f'check full details here:\n{full_log}\n'))

    return output

output.extend(main())

# ============ EMAIL =============
output.append(f.timestamp('end', f.project_name))
output.extend(m.email_log(output, additional_recipients_list=['jakub.pasik@groupon.com','mlugowska@groupon.com','centralmerchwarsaw@groupon.com', 'rev_mgmt_analytics@groupon.com'], how='smtp'))


# ============ EXIT =============
if status_flag:
    exit(0)
else:
    raise Exception(f.project_name+' job failed')