
import pandas as pd

#CSV files to use
file_loc = 'BASE_FILE_LOCATION'

fundingRds_file = file_loc + 'fundingRounds.csv' #[1]
fundingRdsInvest_file = file_loc + 'invest_indiv_fundingRounds.csv' #[2]
fundingRdsCos_file = file_loc + 'orgs_indiv_fundingRounds.csv' #[3]
invstrCos_file = file_loc + 'invstr_company_info.csv' #[4]
invstrPpl_file = file_loc + 'invstr_people_info.csv' #[5]
# [6] -- skipping for now
fundedCos_file = file_loc + 'company_info.csv' #[7]
# [8] -- skipping for now


# STEPS
#  Funded Companies
#   Goal: match detailed round info with the funded companies
#   1) clean dataset #7
#   2) filter dataset #1 to include only 2017 rounds
#   3) merge datset from step 2 with dataset #3
#   4) merge datasets from steps 1 and 3 together
#  Investors
#   Goal: match round URLs with investors (both companies and people)
#   5) merge dataset #4 with dataset #2
#   6) merge dataset #5 with dataset #2


#STEP 1
#cleaning funded org data set
fndCos_df = pd.read_csv(fundedCos_file)

    #dropping unnecessary column that numerical labels each row
fndCos_df = fndCos_df.drop(fndCos_df.columns[0], axis =1)

    #splitting url to remove user key
def url_splitter(x):
    url = x.split('?')
    return(url[0])

fndCos_df['company URL'] = fndCos_df['company URL'].apply(url_splitter)


#STEP 2
#finding only info from the funding rounds occuring in 2017
fndRd_df = pd.read_csv(fundingRds_file)

    #creating year column to facilitate filter
def year_pull(x):
    yr = x.split('/')
    return(yr[-1])

fndRd_df['announce year'] = fndRd_df['announce date'].apply(year_pull)

    #filtering all but 2017 funding rounds
yr = ['2017']
fndRds2017_df = fndRd_df[fndRd_df['announce year'].isin(yr)]


#STEP 3
#matching funding round info to funded org URL
fndRdCos_df = pd.read_csv(fundingRdsCos_file, encoding = 'ISO-8859-1')

    #splitting url to remove user key and facilitate merge
fndRdCos_df['round url'] = fndRdCos_df['round url'].apply(url_splitter)

    #dropping unnecessary column that numerical labels each row
fndRdCos_df = fndRdCos_df.drop(fndRdCos_df.columns[0], axis = 1)

    #merging
fndRdCosFull_df = pd.merge(fndRds2017_df, fndRdCos_df, on = 'round url')


#STEP 4
#merging funded organisation full info with funding round info
fndRdCosFull_df = fndRdCosFull_df.rename(columns={'organisation url': 'company URL'})

fundedCosFull_df = pd.merge(fndCos_df,fndRdCosFull_df, on = 'company URL')                # --------> final funded companies data frame

    #tidying data
tidyFundedCosFull_df = pd.melt(fundedCosFull_df, ['round url'], var_name = 'variable',
                                            value_name = 'value')
tidyFundedCosFull_df = tidyFundedCosFull_df.sort_values(by=['round url'])
tidyFundedCosFull_df.columns = ['round URL', 'variable', 'value']

    #output files
# fundedCosFull_df.to_csv(file_loc + '/Merged/funded_companies_full.csv')
# tidyFundedCosFull_df.to_csv(file_loc + '/Merged/funded_companies_full(tidy).csv')

# -------------------------------------------------------------------------------------


#STEP 5
#matching investor org details to funding round URLs
investRds_df = pd.read_csv(fundingRdsInvest_file)
invstrCos_df = pd.read_csv(invstrCos_file)

    #dropping unnecessary columns that numerical labels each row
investRds_df = investRds_df.drop(investRds_df.columns[0], axis = 1)
invstrCos_df = invstrCos_df.drop(invstrCos_df.columns[0], axis = 1)

    #splitting url to remove user key
investRds_df['round url'] = investRds_df['round url'].apply(url_splitter)

    #rename investor url
invstrCos_df = invstrCos_df.rename(columns={'funding round url': 'investor url'})

    #merging
invstrCoNRd_df = pd.merge(investRds_df, invstrCos_df, on = 'investor url')                # --------> final investors (companies) data frame

    #output file
# invstrCoNRd_df.to_csv(file_loc + '/Merged/investors_companies_full.csv')


#STEP 6
#matching investor people details to funding round URLs
invstrPpl_df = pd.read_csv(invstrPpl_file)

    #dropping unnecessary columns that numerical labels each row
invstrPpl_df = invstrPpl_df.drop(invstrPpl_df.columns[0], axis = 1)

    #rename investor url
invstrPpl_df = invstrPpl_df.rename(columns={'ppl invstr round url': 'investor url'})

    #merging
invstrPplNRd_df = pd.merge(investRds_df, invstrPpl_df, on = 'investor url')             # --------> final investors (people) data frame

    #output file
# invstrPplNRd_df.to_csv(file_loc + '/Merged/investors_people_full.csv')
