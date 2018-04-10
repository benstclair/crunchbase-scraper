
orgTitle = 'orgs_indiv_fundingRounds'
invTitle = 'invest_indiv_fundingRounds'

import pandas as pd
import requests as rq
import json
import random as rd
from time import sleep, time

#base data file and df
fileName = 'FILE_LOCATION'
fundRds_df = pd.read_csv(fileName)

#adding year column
def year_pull(x):
    yr = x.split('/')
    return(yr[-1])
fundRds_df['announce year'] = fundRds_df['announce date'].apply(year_pull)

#filtering out years and storing in new df
yrs = ['2017']    #full (idea) range -- ['2013','2014','2015', '2016', '2017']
fundRds_newDf = fundRds_df[(fundRds_df['announce year'].isin(yrs)) & (fundRds_df['amount raised'].notnull())]

#base df for requests
roundURL = fundRds_newDf['round url']

user_key = 'USER_KEY'

#data containers
orgRndURL_list = []
invRndURL_list =[]
orgURL_list = []
investorURL_list = []

#loop monitoring
start_time = time()
requests = 0

for rnd in roundURL:

    try:
        url = str(rnd) + '?user_key=' + str(user_key)
        response = rq.get(url)

        sleep(rd.randint(0,3))

    #request monitoring (one request = 1 company)
        requests += 1
        elapsed_time = time() - start_time
        print('Request: {}; Total Time: {} min; Frequency: {} sec/req'.format(requests, \
        round(elapsed_time/60,2), round(elapsed_time/requests,2)))
        print(url)
        print('status code: ' + str(response.status_code))

        round_json = json.loads(response.content)

    #control flow in case the link is no longer valid
        while True:
            try:

                investorContainer = round_json['data']['relationships']['investors']['items']
                orgContainer = round_json['data']['relationships']['funded_organization']['item']

            #appending data

                #org appending
                orgURL = orgContainer['properties']['api_url']
                orgURL_list.append(orgURL)
                orgRndURL_list.append(url)

                #investor appending
                for investor in investorContainer:
                    invNum_range = [i for i in range(0,(len(investorContainer)))]
                    for num in invNum_range:
                        investorURL = investorContainer[num]['properties']['api_url']
                        investorURL_list.append(investorURL)
                        invRndURL_list.append(url)
                    break
                break
            except TypeError:
                orgURL_list.append('N/A')
                orgRndURL_list.append(url)
                investorURL_list.append('N/A')
                invRndURL_list.append(url)
                break
    except:
        orgURL_list.append('N/A')
        orgRndURL_list.append(url)
        investorURL_list.append('N/A')
        invRndURL_list.append(url)

#dataframe creation and CSV export
    org_df = pd.DataFrame({'round url': orgRndURL_list,
                       'organisation url': orgURL_list})

    investor_df = pd.DataFrame({'round url': invRndURL_list,
                                'investor url': investorURL_list})

    org_df.to_csv('OUTPUT_LOCATION' + str(orgTitle) + '.csv')

    investor_df.to_csv('OUTPUT_LOCATION' + str(invTitle) + '.csv')
