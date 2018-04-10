
title = 'fundingRounds'

import pandas as pd
import requests as rq
import json
import random as rd
from time import sleep, time

#url details
user_key = 'USER_KEY'

url = 'https://api.crunchbase.com/v3.1/funding-rounds?page=1&items_per_page=250&user_key=' + str(user_key)

response = rq.get(url)

fundRds_json = json.loads(response.content)

#determining # of pages
nPages = fundRds_json['data']['paging']['number_of_pages']
page_range = list(range(1,(nPages + 1)))

#data containers
roundURL = []
typeRd = []
seriesAlpha = []
seriesQual = []
dateAnnoun = []
dateAnnounTC = []
dateClsd = []
dateClsdTC = []
amtRsd = []
amtCurrn = []
amtRsdUSD = []
trgMny = []
trgMnyCrn = []
trgMnyUSD = []
preVal = []
preValCrn = []
preValUSD = []

#loop monitoring
start_time = time()
requests = 0

#iteration per page
for page in page_range:

#new url and response for each page
    urlNew = 'https://api.crunchbase.com/v3.1/funding-rounds?page=' + str(page) + '&items_per_page=250&user_key=' + str(user_key)
    responseNew = rq.get(urlNew)

    sleep(rd.randint(4,14))

#request monitoring (one request = 1 page)
    requests += 1
    elapsed_time = time() - start_time
    print('Request: {}; Total Time: {} min; Frequency: {} sec/req'.format(requests,round(elapsed_time/60,2), \
    round(elapsed_time/requests,2)))
    print(urlNew)

    fundRdsNew_json = json.loads(responseNew.content)

    fundRds_container = fundRdsNew_json['data']['items']

    for fundRd in fundRds_container:

    #iterating tool to grab each 'item' in a page's container
        num_range = [i for i in range(0,(len(fundRds_container)))]

        for num in num_range:

            rnd = fundRds_container[num]['properties']['api_url']
            roundURL.append(rnd)

            tp = fundRds_container[num]['properties']['funding_type']
            typeRd.append(tp)

            seriesAl = fundRds_container[num]['properties']['series']
            seriesAlpha.append(seriesAl)

            qual = fundRds_container[num]['properties']['series_qualifier']
            seriesQual.append(qual)

            announced = fundRds_container[num]['properties']['announced_on']
            dateAnnoun.append(announced)

            aTC = fundRds_container[num]['properties']['announced_on_trust_code']
            dateAnnounTC.append(aTC)

            closed = fundRds_container[num]['properties']['closed_on']
            dateClsd.append(closed)

            cTC = fundRds_container[num]['properties']['closed_on_trust_code']
            dateClsdTC.append(cTC)

            amt = fundRds_container[num]['properties']['money_raised']
            amtRsd.append(amt)

            currency = fundRds_container[num]['properties']['money_raised_currency_code']
            amtCurrn.append(currency)

            usd = fundRds_container[num]['properties']['money_raised_usd']
            amtRsdUSD.append(usd)

            tm = fundRds_container[num]['properties']['target_money_raised']
            trgMny.append(tm)

            tmcur = fundRds_container[num]['properties']['target_money_raised_currency_code']
            trgMnyCrn.append(tmcur)

            tusd = fundRds_container[num]['properties']['target_money_raised_usd']
            trgMnyUSD.append(tusd)

            val = fundRds_container[num]['properties']['pre_money_valuation']
            preVal.append(val)

            valcrn = fundRds_container[num]['properties']['pre_money_valuation_currency_code']
            preValCrn.append(valcrn)

            valusd = fundRds_container[num]['properties']['pre_money_valuation_usd']
            preValUSD.append(valusd)

        break

#dataframe creation and CSV export
df = pd.DataFrame({'round url': roundURL,
                   'round type': typeRd,
                   'series': seriesAlpha,
                   'series qual': seriesQual,
                   'announce date': dateAnnoun,
                   'annouce trust code': dateAnnounTC,
                   'date closed': dateClsd,
                   'close trust code': dateClsdTC,
                   'amount raised': amtRsd,
                   'amount raised currency': amtCurrn,
                   'amount raised USD': amtRsdUSD,
                   'target money': trgMny,
                   'target money currency': trgMnyCrn,
                   'target money USD': trgMnyUSD,
                   'pre money valuation': preVal,
                   'pre money val currency': preValCrn,
                   'pre money val USD': preValUSD})

df.to_csv('OUTPUT_FILE_LOCATION' + str(title) + '.csv')
