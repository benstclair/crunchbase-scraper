
import pandas as pd

#base file location
file_loc = 'BASE_FILE_LOCATION'

#base pandas data frames
fundedCos_df = pd.read_csv(file_loc + 'funded_companies_full.csv')
investorCos_df = pd.read_csv(file_loc + 'investors_companies_full.csv')
investorPpl_df = pd.read_csv(file_loc + 'investors_people_full.csv')


# FUNDED COMPANIES

    #slimming down columns for funded companies data
fundedCos_df = fundedCos_df.rename(columns={'company name': 'fundedCompany_name',
                                            'HQ city': 'fundedCompany_city',
                                            'HQ country': 'fundedCompany_country',
                                            'founded date': 'fundedCompany_foundedDate',
                                            'round type': 'fundedCompany_roundType',
                                            'amount raised USD': 'fundedCompany_amntRaisedUSD',
                                            'announce date': 'fundedCompany_roundAnnounceDate',
                                            '# funding rounds': 'fundedCompany_numFundingRounds',
                                            '# investors': 'fundedCompany_numInvestors',
                                            'round url': 'fundedCompany_roundUrl'})

    #creating year column
def year_pull(x):
    yr = str(x).split('-')
    return(yr[0])

fundedCos_df['fundedCompany_foundedYear'] = fundedCos_df['fundedCompany_foundedDate'].apply(year_pull)

    #formatting currency and adding column
def amnt_format(x):
    return('${:,.2f}'.format(x, ','))

fundedCos_df['fundedCompany_amntRaisedUSD_real'] = fundedCos_df['fundedCompany_amntRaisedUSD'].apply(amnt_format)

    #formatting round URL to work in browser
def url_converter(x):
    url = str(x).split('/')
    return('https://www.crunchbase.com/funding-round/' + str(url[-1]))

fundedCos_df['fundedCompany_roundUrl_real'] = fundedCos_df['fundedCompany_roundUrl'].apply(url_converter)

fundedCos_slimDF = fundedCos_df.filter(['fundedCompany_name',
                                    'fundedCompany_city',
                                    'fundedCompany_country',
                                    'fundedCompany_foundedDate',
                                    'fundedCompany_foundedYear',
                                    'fundedCompany_roundType',
                                    'fundedCompany_amntRaisedUSD',
                                    'fundedCompany_amntRaisedUSD_real',
                                    'fundedCompany_roundAnnounceDate',
                                    'fundedCompany_numFundingRounds',
                                    'fundedCompany_numInvestors',
                                    'fundedCompany_roundUrl',
                                    'fundedCompany_roundUrl_real'], axis = 1)

    #finding only European companies recieving funding in 2017
euroCountries = ['Albania', 'Andorra', 'Armenia', 'Austria', 'Azerbaijan', 'Belarus',
'Belgium', 'Bosnia Herzegovina', 'Bulgaria', 'Croatia', 'Cyprus', 'Czech Republic',
'Denmark', 'Estonia', 'Finland', 'France', 'Georgia', 'Germany', 'Greece', 'Hungary',
'Iceland', 'Ireland', 'Italy', 'Kazakhstan', 'Kosovo', 'Latvia', 'Liechtenstein',
'Lithuania', 'Luxembourg', 'Macedonia', 'Malta', 'Moldova', 'Monaco', 'Montenegro',
'Netherlands', 'Norway', 'Poland', 'Portugal', 'Romania', 'Russia', 'San Marino',
'Serbia', 'Slovakia', 'Slovenia', 'Spain', 'Sweden', 'Switzerland', 'Turkey',
'Ukraine', 'United Kingdom', 'Vatican City']

    #filtering to include UK only
fundedUKCos_slimDF = fundedCos_slimDF[fundedCos_slimDF['fundedCompany_country'] == 'United Kingdom']

    #filtering for companies founded since 2014
yrs = ['2014', '2015', '2016', '2017']
fundedUKCos_slimDF = fundedUKCos_slimDF[fundedUKCos_slimDF.fundedCompany_foundedYear.isin(yrs)]

    #output file
fundedUKCos_slimDF.to_csv(file_loc + '/Condensed/fundedUKStrtUpAll.csv')


# INVESTORRS

    #slimming down columns for data on investors - companies
investorCos_df = investorCos_df.rename(columns={'org invstr name': 'investorCompany_name',
                                                'org invstr hq city': 'investorCompany_city',
                                                'org invstr hq country': 'investorCompany_country',
                                                'round url': 'investorCompany_roundUrl'})

investorCos_slimDF = investorCos_df.filter(['investorCompany_name',
                                            'investorCompany_city',
                                            'investorCompany_country',
                                            'investorCompany_roundUrl'], axis = 1)

    #filering only the rounds from UK companies
UKRoundUrls_list = list(fundedUKCos_slimDF['fundedCompany_roundUrl'])

investorsCosUKRds_slimDF = investorCos_slimDF[investorCos_slimDF.investorCompany_roundUrl.isin(UKRoundUrls_list)]

    #output file
#investorsCosUKRds_slimDF.to_csv(file_loc + '/Condensed/investorsCosUKStrtUpRds.csv')


    #slimming down columns for data on investors - people
investorPpl_df['investorPeople_full_name'] = investorPpl_df['ppl invstr first name'].map(str) + ' ' + \
                                              investorPpl_df['ppl invstr last name']

investorPpl_df = investorPpl_df.rename(columns={'ppl invstr organisation': 'investorPeople_company',
                                                'ppl invstr job title': 'investorPeople_jobTitle',
                                                'ppl invstr gender': 'investorPeople_gender',
                                                'ppl invstr city': 'investorPeople_city',
                                                'ppl invstr country': 'investorPeople_country',
                                                'ppl invstr continent': 'investorPeople_continent',
                                                'round url': 'investorPeople_roundUrl'})

investorsPpl_slimDF = investorPpl_df.filter(['investorPeople_full_name',
                                             'investorPeople_company',
                                             'investorPeople_jobTitle',
                                             'investorPeople_gender',
                                             'investorPeople_city',
                                             'investorPeople_country',
                                             'investorPeople_continent',
                                             'investorPeople_roundUrl'], axis = 1)

    #filering only the rounds from UK companies
investorsPplUKRds_slimDF = investorsPpl_slimDF[investorsPpl_slimDF.investorPeople_roundUrl.isin(UKRoundUrls_list)]


    #output file
#investorsPplUKRds_slimDF.to_csv(file_loc + '/Condensed/investorsPplUKStrtUpRds.csv')
