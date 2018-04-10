
invstr_company_title = 'invstr_company_info'
invstr_people_title = 'invstr_people_info'
invstr_company_categ = 'invsr_company_categs'

import pandas as pd
import requests as rq
import json
import random as rd
from time import sleep, time

#base data file and df
fileName = 'FILE_NAME'
investors_df = pd.read_csv(fileName)

#base df for requests
investors_URL = investors_df['investor url'].drop_duplicates()

user_key = 'USER_KEY'

#data containers - organisation investors
orgInvstRdURL_list = []
orgInvstrURL_list = []

orgInvstName_list = []
orgInvstShrtDesc_list = []
orgInvstFndDate_list = []
orgInvstFndTC_list = []
orgInvstClsd_list = []
orgInvstEmployMin_list = []
orgInvstEmployMax_list = []
orgInvstStockEx_list = []
orgInvstStockSym_list = []
totFundingUSD_list = []
orgInvstNoInvstmnts_list = []

orgInvstHQSt1_list = []
orgInvstHQSt2_list = []
orgInvstHQPost_list = []
orgInvstHQCity_list = []
orgInvstHQRegion_list = []
orgInvstHQCountry_list = []
orgInvstHQLat_list = []
orgInvstHQLong_list = []

orgInvstNoAcq_list = []
orgInvstIPODate_list = []

#data containers - people investors
pplInvstRdURL_list = []
pplInvstrURL_list = []
pplInvstrFirstName_list = []
pplInvstrLastName_list = []
pplInvstrGender_list = []
pplInvstrBio_list = []
pplInvstrBornOn_list = []
pplInvstrBornTC_list = []
pplInvstrDiedOn_list = []
pplInvstrDiedTC_list = []

pplInvstrNoInvsts_list = []
pplInvstrCity_list = []
pplInvstrRegion_list = []
pplInvstrCountry_list = []
pplInvstrContinent_list = []
pplInvstrJob_list = []
pplInvstrOrg_list = []

#categories containers - both people and organizations
orgInvstrURLCat = []
orgInvstCats = []

#loop monitoring
start_time = time()
requests = 0

for invstr in investors_URL:

    try:

        url = str(invstr) + '?user_key=' + str(user_key)
        response = rq.get(url)

        sleep(rd.randint(0,3))

    #request monitoring (one request = 1 company)
        requests += 1
        elapsed_time = time() - start_time
        print('Request: {}; Total Time: {} min; Frequency: {} sec/req'.format(requests, \
        round(elapsed_time/60,2), round(elapsed_time/requests,2)))
        print(url)
        print('status code: ' + str(response.status_code))

        invstr_json = json.loads(response.content)
        invstr_type = invstr_json['data']['type']

        if invstr_type == 'Organization':

        #org investor properties container/data
            invstr_org_props = invstr_json['data']['properties']

            orgInvstRdURL_list.append(invstr)
            orgInvstrURL_list.append(url)

            name =invstr_org_props['name']
            orgInvstName_list.append(name)

            shrtDesc = invstr_org_props['short_description']
            orgInvstShrtDesc_list.append(shrtDesc)

            founded = invstr_org_props['founded_on']
            orgInvstFndDate_list.append(founded)

            foundedTC = invstr_org_props['founded_on_trust_code']
            orgInvstFndTC_list.append(foundedTC)

            closed = invstr_org_props['is_closed']
            orgInvstClsd_list.append(closed)

            employMin = invstr_org_props['num_employees_min']
            orgInvstEmployMin_list.append(employMin)

            employMax = invstr_org_props['num_employees_max']
            orgInvstEmployMax_list.append(employMax)

            stockEx = invstr_org_props['stock_exchange']
            orgInvstStockEx_list.append(stockEx)

            stockSym = invstr_org_props['stock_symbol']
            orgInvstStockSym_list.append(stockSym)

            totFundUSD = invstr_org_props['total_funding_usd']
            totFundingUSD_list.append(totFundUSD)

            noInvests = invstr_org_props['number_of_investments']
            orgInvstNoInvstmnts_list.append(noInvests)

        #org relationships container/data
            invstr_org_relation = invstr_json['data']['relationships']

            noAcqs = invstr_org_relation['acquisitions']['paging']['total_items']
            orgInvstNoAcq_list.append(noAcqs)

            try:
                IPOdate = invstr_org_relation['ipo']['item']['properties']['went_public_on']
                orgInvstIPODate_list.append(IPOdate)
            except:
                orgInvstIPODate_list.append('N/A')

            try:
            #org HQ container/data
                invstr_hq = invstr_org_relation['headquarters']['item']['properties']

                hqSt1 = invstr_hq['street_1']
                orgInvstHQSt1_list.append(hqSt1)

                hqSt2 = invstr_hq['street_2']
                orgInvstHQSt2_list.append(hqSt2)

                hqPost = invstr_hq['postal_code']
                orgInvstHQPost_list.append(hqPost)

                hqCity = invstr_hq['city']
                orgInvstHQCity_list.append(hqCity)

                hqRegion = invstr_hq['region']
                orgInvstHQRegion_list.append(hqRegion)

                hqCountry = invstr_hq['country']
                orgInvstHQCountry_list.append(hqCountry)

                hqLat = invstr_hq['latitude']
                orgInvstHQLat_list.append(hqLat)

                hqLong = invstr_hq['longitude']
                orgInvstHQLong_list.append(hqLong)

            except:
                orgInvstHQSt1_list.append('N/A')
                orgInvstHQSt2_list.append('N/A')
                orgInvstHQPost_list.append('N/A')
                orgInvstHQCity_list.append('N/A')
                orgInvstHQRegion_list.append('N/A')
                orgInvstHQCountry_list.append('N/A')
                orgInvstHQLat_list.append('N/A')
                orgInvstHQLong_list.append('N/A')

            #investor org category container/data
            invstCatContainer = invstr_org_relation['categories']['items']
            num_range = [i for i in range(0, (len(invstCatContainer)))]
            for num in num_range:
                try:
                    categ = invstCatContainer[num]['properties']['name']
                    orgInvstCats.append(categ)
                    orgInvstrURLCat.append(url)
                except:
                    orgInvstCats.append('N/A')
                    orgInvstrURLCat.append(url)

        elif invstr_type == 'Person':

            #ppl investor properties container/data
            invstr_ppl_props = invstr_json['data']['properties']

            pplInvstRdURL_list.append(invstr)
            pplInvstrURL_list.append(url)

            firstName = invstr_ppl_props['first_name']
            pplInvstrFirstName_list.append(firstName)

            lastName = invstr_ppl_props['last_name']
            pplInvstrLastName_list.append(lastName)

            gender = invstr_ppl_props['gender']
            pplInvstrGender_list.append(gender)

            bio = invstr_ppl_props['bio']
            pplInvstrBio_list.append(bio)

            born = invstr_ppl_props['born_on']
            pplInvstrBornOn_list.append(born)

            bornTC = invstr_ppl_props['born_on_trust_code']
            pplInvstrBornTC_list.append(bornTC)

            death = invstr_ppl_props['died_on']
            pplInvstrDiedOn_list.append(death)

            deathTC = invstr_ppl_props['died_on_trust_code']
            pplInvstrDiedTC_list.append(deathTC)

            #ppl investor relationships container/data
            invstr_ppl_relation = invstr_json['data']['relationships']

            try:
                pplNoInvsts = invstr_ppl_relation['investments']['paging']['total_items']
                pplInvstrNoInvsts_list.append(pplNoInvsts)
            except:
                pplInvstrNoInvsts_list.append('N/A')

            try:
                pplJob = invstr_ppl_relation['primary_affiliation']['item']['properties']['title']
                pplInvstrJob_list.append(pplJob)
            except:
                pplInvstrJob_list.append('N/A')

            try:
                pplOrg = invstr_ppl_relation['primary_affiliation']['item']['relationships']['organization']['properties']['name']
                pplInvstrOrg_list.append(pplOrg)
            except:
                pplInvstrOrg_list.append('N/A')

            try:
            #ppl investor location containter/data
                invstr_ppl_location = invstr_ppl_relation['primary_location']['item']['properties']

                pplCity = invstr_ppl_location['city']
                pplInvstrCity_list.append(pplCity)

                pplRegion = invstr_ppl_location['region']
                pplInvstrRegion_list.append(pplRegion)

                pplCountry = invstr_ppl_location['country']
                pplInvstrCountry_list.append(pplCountry)

                pplContinent = invstr_ppl_location['continent']
                pplInvstrContinent_list.append(pplContinent)

            except:
                pplInvstrCity_list.append('N/A')
                pplInvstrRegion_list.append('N/A')
                pplInvstrCountry_list.append('N/A')
                pplInvstrContinent_list.append('N/A')

    except:
        #org lists
            orgInvstRdURL_list.append('N/A')
            orgInvstrURL_list.append('N/A')
            orgInvstName_list.append('N/A')
            orgInvstShrtDesc_list.append('N/A')
            orgInvstFndDate_list.append('N/A')
            orgInvstFndTC_list.append('N/A')
            orgInvstClsd_list.append('N/A')
            orgInvstEmployMin_list.append('N/A')
            orgInvstEmployMax_list.append('N/A')
            orgInvstStockEx_list.append('N/A')
            orgInvstStockSym_list.append('N/A')
            totFundingUSD_list.append('N/A')
            orgInvstNoInvstmnts_list.append('N/A')
            orgInvstHQSt1_list.append('N/A')
            orgInvstHQSt2_list.append('N/A')
            orgInvstHQPost_list.append('N/A')
            orgInvstHQCity_list.append('N/A')
            orgInvstHQRegion_list.append('N/A')
            orgInvstHQCountry_list.append('N/A')
            orgInvstHQLat_list.append('N/A')
            orgInvstHQLong_list.append('N/A')
            orgInvstNoAcq_list.append('N/A')
            orgInvstIPODate_list.append('N/A')
        #ppl lists
            pplInvstRdURL_list.append('N/A')
            pplInvstrURL_list.append('N/A')
            pplInvstrFirstName_list.append('N/A')
            pplInvstrLastName_list.append('N/A')
            pplInvstrGender_list.append('N/A')
            pplInvstrBio_list.append('N/A')
            pplInvstrBornOn_list.append('N/A')
            pplInvstrBornTC_list.append('N/A')
            pplInvstrDiedOn_list.append('N/A')
            pplInvstrDiedTC_list.append('N/A')
            pplInvstrNoInvsts_list.append('N/A')
            pplInvstrCity_list.append('N/A')
            pplInvstrRegion_list.append('N/A')
            pplInvstrCountry_list.append('N/A')
            pplInvstrContinent_list.append('N/A')
            pplInvstrJob_list.append('N/A')
            pplInvstrOrg_list.append('N/A')
        #categories list
            orgInvstrURLCat.append('N/A')
            orgInvstCats.append('N/A')


    #data frames and CSV exports
    invstr_org_df = pd.DataFrame({'funding round url': orgInvstRdURL_list,
                                  'org invstr url': orgInvstrURL_list,
                                  'org invstr name':orgInvstName_list,
                                  'org invstr desc': orgInvstShrtDesc_list,
                                  'org invstr founded date': orgInvstFndDate_list,
                                  'org invstr fnd date TC': orgInvstFndTC_list,
                                  'org invstr clsd date': orgInvstClsd_list,
                                  'org invstr clsd date TC': orgInvstEmployMin_list,
                                  'org invstr employ max': orgInvstEmployMax_list,
                                  'org invstr stock ex': orgInvstStockEx_list,
                                  'org invstr stock symbol': orgInvstStockSym_list,
                                  'org invstr total funding usd': totFundingUSD_list,
                                  'org invstr # investments': orgInvstNoInvstmnts_list,
                                  'org invstr HQ st1': orgInvstHQSt1_list,
                                  'org invstr hq st2': orgInvstHQSt2_list,
                                  'org invstr hq post': orgInvstHQPost_list,
                                  'org invstr hq city': orgInvstHQCity_list,
                                  'org invstr hq region': orgInvstHQRegion_list,
                                  'org invstr hq country': orgInvstHQCountry_list,
                                  'org invstr hq lat': orgInvstHQLat_list,
                                  'org invstr long': orgInvstHQLong_list,
                                  'org invstr # acqs': orgInvstNoAcq_list,
                                  'org invstr IPO date': orgInvstIPODate_list})

    invstr_ppl_df = pd.DataFrame({ 'ppl invstr round url': pplInvstRdURL_list,
                                   'ppl invstr url': pplInvstrURL_list,
                                   'ppl invstr first name': pplInvstrFirstName_list,
                                   'ppl invstr last name': pplInvstrLastName_list,
                                   'ppl invstr gender': pplInvstrGender_list,
                                   'ppl invstr bio': pplInvstrBio_list,
                                   'ppl invstr born on': pplInvstrBornOn_list,
                                   'ppl invstr born TC': pplInvstrBornTC_list,
                                   'ppl invstr died on': pplInvstrDiedOn_list,
                                   'ppl invstr died TC': pplInvstrDiedTC_list,
                                   'ppl invstr # investments': pplInvstrNoInvsts_list,
                                   'ppl invstr city': pplInvstrCity_list,
                                   'ppl invstr region': pplInvstrRegion_list,
                                   'ppl invstr country': pplInvstrCountry_list,
                                   'ppl invstr continent': pplInvstrContinent_list,
                                   'ppl invstr job title': pplInvstrJob_list,
                                   'ppl invstr organisation': pplInvstrOrg_list})

    invstrOrg_cat_df = pd.DataFrame({'org invstr url': orgInvstrURLCat,
                                  'org invstr category': orgInvstCats})

    invstr_ppl_df.to_csv('FILE_LOCATION' + str(invstr_people_title) + '.csv')
    invstr_org_df.to_csv('FILE_LOCATION' + str(invstr_company_title) + '.csv')
    invstrOrg_cat_df.to_csv('FILE_LOCATION' + str(invstr_company_categ) + '.csv')
