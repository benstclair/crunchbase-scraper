
co_info_title = 'company_info'
categ_title = 'company_info_categ'


import pandas as pd
import requests as rq
import json
import random as rd
from time import sleep, time

#base data file
fileName = 'FILE_NAME'

cos_df = pd.read_csv(fileName, encoding = 'ISO-8859-1')
coNameURL = cos_df['organisation url'].drop_duplicates()

user_key = 'USER_KEY'

#data containers - funded_co
coName_list = []
coURL_list = []
coShrtDesc_list = []
coFoundedDate_list = []
coFndDtTC_list = []
coClsd_list = []
#coClsdDtTC_list = []
coEmployeesMin_list = []
coEmployeesMax_list = []
coStockEx_list = []
coStockSymb_list = []
coTotalFundingUSD_list = []
coNoInvestors_list = []
coNoInvestments_list = []
coNoAcquisitions_list = []
noFundRounds_list = []
coHQSt1 = []
coHQSt2 = []
coHQCity = []
coHQRegion = []
coHQCountry = []
coHQPostCode = []
coHQLat = []
coHQLong = []

#category containers - funded_co
coURL_cat_list = []
coCategory = []

#loop monitoring
start_time = time()
requests = 0

for name in coNameURL:

    try:
        url = str(name) + '?user_key=' + str(user_key)
        response = rq.get(url)

        sleep(rd.randint(0,3))

    #request monitoring (one request = 1 company)
        requests += 1
        elapsed_time = time() - start_time
        print('Request: {}; Total Time: {} min; Frequency: {} sec/req'.format(requests, round(elapsed_time/60,2), \
        round(elapsed_time/requests,2)))
        print(url)
        print(response.status_code)

        cos_json = json.loads(response.content)

        #containers
        propsContainer = cos_json['data']['properties']
        relationContainer = cos_json['data']['relationships']
        catContainer = relationContainer['categories']['items']

        #from propsContainer
        name = propsContainer['name']
        coName_list.append(name)

        coURL_list.append(url)

        desc = propsContainer['short_description']
        coShrtDesc_list.append(desc)

        founded = propsContainer['founded_on']
        coFoundedDate_list.append(founded)

        foundedTC = propsContainer['founded_on_trust_code']
        coFndDtTC_list.append(foundedTC)

        closed = propsContainer['is_closed']
        coClsd_list.append(closed)

        employeesMin = propsContainer['num_employees_min']
        coEmployeesMin_list.append(employeesMin)

        employeesMax = propsContainer['num_employees_max']
        coEmployeesMax_list.append(employeesMax)

        stockEx = propsContainer['stock_exchange']
        coStockEx_list.append(stockEx)

        stockSym = propsContainer['stock_symbol']
        coStockSymb_list.append(stockSym)

        funding = propsContainer['total_funding_usd']
        coTotalFundingUSD_list.append(funding)

        #from relationContainer
        noInvestors = relationContainer['investors']['paging']['total_items']
        coNoInvestors_list.append(noInvestors)

        noInvestments = relationContainer['investments']['paging']['total_items']
        coNoInvestments_list.append(noInvestments)

        noAcquis = relationContainer['acquisitions']['paging']['total_items']
        coNoAcquisitions_list.append(noAcquis)

        noRds = relationContainer['funding_rounds']['paging']['total_items']
        noFundRounds_list.append(noRds)

        #from hqContainer
        try:
            hqContainer = relationContainer['headquarters']['item']['properties']

            st1 = hqContainer['street_1']
            coHQSt1.append(st1)

            st2 = hqContainer['street_2']
            coHQSt2.append(st2)

            hqCity = hqContainer['city']
            coHQCity.append(hqCity)

            hqregion = hqContainer['region']
            coHQRegion.append(hqregion)

            hqCountry = hqContainer['country']
            coHQCountry.append(hqCountry)

            hqPC = hqContainer['postal_code']
            coHQPostCode.append(hqPC)

            lat = hqContainer['latitude']
            coHQLat.append(lat)

            long = hqContainer['longitude']
            coHQLong.append(long)

        except KeyError:
            coHQSt1.append('N/A')
            coHQSt2.append('N/A')
            coHQCity.append('N/A')
            coHQRegion.append('N/A')
            coHQCountry.append('N/A')
            coHQPostCode.append('N/A')
            coHQLat.append('N/A')
            coHQLong.append('N/A')

        #categories
        num_range = [i for i in range(0, (len(catContainer)))]
        for num in num_range:
            try:
                categ = catContainer[num]['properties']['name']
                coCategory.append(categ)
                coURL_cat_list.append(url)
            except:
                coCategory.append('N/A')
                coURL_cat_list.append(url)

    except:
        coName_list.append('N/A')
        coURL_list.append(url)
        coShrtDesc_list.append('N/A')
        coFoundedDate_list.append('N/A')
        coFndDtTC_list.append('N/A')
        coClsd_list.append('N/A')
        #coClsdDtTC_list = []
        coEmployeesMin_list.append('N/A')
        coEmployeesMax_list.append('N/A')
        coStockEx_list.append('N/A')
        coStockSymb_list.append('N/A')
        coTotalFundingUSD_list.append('N/A')
        coNoInvestors_list.append('N/A')
        coNoInvestments_list.append('N/A')
        coNoAcquisitions_list.append('N/A')
        noFundRounds_list.append('N/A')
        coHQSt1.append('N/A')
        coHQSt2.append('N/A')
        coHQCity.append('N/A')
        coHQRegion.append('N/A')
        coHQCountry.append('N/A')
        coHQPostCode.append('N/A')
        coHQLat.append('N/A')
        coHQLong.append('N/A')


    #dataframe creation and CSV export
    df_coInfo = pd.DataFrame({'company name': coName_list,
                            'company URL': coURL_list,
                            'short description': coShrtDesc_list,
                            'founded date': coFoundedDate_list,
                            'founded trust code': coFndDtTC_list,
                            'closed date': coClsd_list,
                    #        'closed trust code': coClsdDtTC_list,
                            'min employees': coEmployeesMin_list,
                            'max employees': coEmployeesMax_list,
                            'stock exchange': coStockEx_list,
                            'stock symbol': coStockSymb_list,
                            'total funding USD': coTotalFundingUSD_list,
                            '# investors': coNoInvestors_list,
                            '# funding rounds': noFundRounds_list,
                            '# investments': coNoInvestments_list,
                            '# acquisitions': coNoAcquisitions_list,
                            'HQ Street 1': coHQSt1,
                            'HQ Street 2': coHQSt2,
                            'HQ city': coHQCity,
                            'HQ region': coHQRegion,
                            'HQ country': coHQCountry,
                            'HQ post code': coHQPostCode,
                            'HQ lat': coHQLat,
                            'HQ long': coHQLong})


    df_categories = pd.DataFrame({'company url': coURL_cat_list,
                                  'category': coCategory})

    df_categories.to_csv('FILE_OUTPUT_LOCATION' + str(categ_title) + '.csv')

    df_coInfo.to_csv('FILE_OUTPUT_LOCATION + str(co_info_title) + '.csv')
