import os

import pandas as pd
from utility_data.crosswalks import state_identifiers

list_of_titles = ['RETAIL DRUG DISTRIBUTION BY ZIP CODE FOR EACH STATE',
                  'RETAIL DRUG DISTRIBUTION BY ZIP CODE WITHIN STATE BY GRAMS WT',
                  'RETAIL DRUG DISTRIBUTION BY ZIP CODE WITHIN STATE BY GRAMS WEIGHT',
                  'RETAIL DRUG DISTRIBUTION BY ZIP CODE WITHIN STATE',
                  'RETAIL DRUG DISTRIBUTION BY DRUG CODE - TOTAL U.S. (IN GRAMS)',
                  'RETAIL DRUG DISTRIBUTION BY STATE WITHIN DRUG CODE BY GRAMS WT',
                  'RETAIL DRUG DISTRIBUTION BY STATE WITHIN DRUG CODE BY GRAMS WEIGHT',
                  'RETAIL DRUG DISTRIBUTION BY STATE WITHIN DRUG CODE',
                  'RETAIL DRUG DISTRIBUTION BY DRUG CODE – TOTAL U.S. (IN GRAMS)',
                  'QUARTERLY DRUG DISTRIBUTION BY STATE PER 100K POPULATION BY GRAM WT',
                  'QUARTERLY DRUG DISTRIBUTION BY STATE PER 100,000 POPULATION BY GRAMS WEIGHT',
                  'QUARTERLY DRUG DISTRIBUTION BY STATE / 100K POPULATION BY MILIGRAM WT',
                  'QUARTERLY DRUG DISTRIBUTION BY STATE PER 100K POPULATION',
                  'QUARTERLY DISTRIBUTION IN GRAMS PER 100,000 POPULATION',
                  'CUMULATIVE DISTRIBUTION BY STATE PER 100K POPULATION',
                  'CUMULATIVE DISTRIBUTION BY STATE IN GRAMS PER 100,000 POPULATION',
                  'CUMULATIVE DISTRIBUTION IN GRAMS PER 100,000 POPULATION',
                  'STATISTICAL SUMMARY FOR RETAIL DRUG PURCHASES BY GRAMS WT',
                  'STATISTICAL SUMMARY FOR RETAIL DRUG PURCHASES BY GRAMS WEIGHT',
                  'STATISTICAL SUMMARY FOR RETAIL DRUG PURCHASES',
                  'UNITED STATES SUMMARY FOR RETAIL DRUG PURCHASES BY GRAMS WT',
                  'UNITED STATES SUMMARY FOR RETAIL DRUG PURCHASES BY GRAMS WEIGHT',
                  'UNITED STATES SUMMARY FOR RETAIL DRUG PURCHASES',
                  'U.S. SUMMARY OF RETAIL DRUG PURCHASES']

totallist = ['UNITED STATES TOTAL',
             'UNITED STATES',
             'U.S. GRAMS / PER 100K POPULATION',
             'U.S. GRAMS / PER 100K:',
             'U.S. GRAMS/100K POP:',
             'U.S. TOTAL',
             'TOTAL',
             'STATE TOTAL',
             'DRUG TOTAL',
             'T0TAL']

statelist = ['TRUST TERRITORIES (GUAM)', 'DISTRICT OF COLUMBIA', 'VERMONT',
             'MICHIGAN', 'OREGON', 'PENNSYLVANIA', 'MASSACHUSETTS', 'ALABAMA',
             'CONNECTICUT', 'VIRGINIA', 'LOUISIANA', 'SOUTH DAKOTA',
             'NEW MEXICO', 'WEST VIRGINIA', 'FLORIDA', 'RHODE ISLAND', 'GUAM',
             'TEXAS', 'SOUTH CAROLINA', 'NEW HAMPSHIRE', 'NORTH DAKOTA',
             'WYOMING', 'ARKANSAS', 'IOWA', 'MINNESOTA', 'HAWAII', 'NEBRASKA',
             'COLORADO', 'ILLINOIS', 'PUERTO RICO', 'TENNESSEE',
             'NORTH CAROLINA', 'MARYLAND', 'KENTUCKY', 'IDAHO', 'UTAH', 'OHIO',
             'INDIANA', 'MISSOURI', 'DELAWARE', 'ALASKA', 'MONTANA',
             'WISCONSIN', 'MAINE', 'KANSAS', 'VIRGIN ISLANDS', 'WASHINGTON',
             'CALIFORNIA', 'ARIZONA', 'AMERICAN SAMOA', 'NEW JERSEY',
             'NEW YORK', 'GEORGIA', 'OKLAHOMA', 'NEVADA', 'MISSISSIPPI']

quarterlist = [['QUARTER 1', 'QUARTER 2', 'QUARTER 3', 'QUARTER 4', 'TOTAL GRAMS'],
               ['1ST QUARTER', '2ND QUARTER', '3RD QUARTER', '4TH QUARTER', 'TOTAL GRAMS'],
               ['1ST QUARTER', '2ND QUARTER', '3RD QUARTER', '4TH QUARTER', 'TOTAL TO DATE'],
               ['1ST QUARTER', '2ND QUARTER', '3RD QUARTER', '4TH QUARTER'],
               ['Q1 CALC DOSAGE UNITS', 'Q2 CALC DOSAGE UNITS', 'Q3 CALC DOSAGE UNITS', 'Q4 CALC DOSAGE UNITS', 'TOTAL GRAMS'],
               ['Q1 CALC GRAMS', 'Q2 CALC GRAMS', 'Q3 CALC GRAMS', 'Q4 CALC GRAMS', 'TOTAL GRAMS'],
               ['RANK', 'STATE', '2000 POP', 'TOTAL GRAMS', 'GRAMS/100K POP'],
               ['RANK', 'STATE', '2010 POP', 'TOTAL GRAMS', 'GRAMS/100K POP'],
               ['RANK', 'STATE NAME', 'STATE POPULATION', 'TOTAL GRAMS', 'TOTAL GRAMS/100K POPULATION'],
               ['DRUG NAME', 'DRUG CODE', 'BUYERS', 'TOTAL GRAMS', 'AVG GRAMS'],
               ['DRUG NAME', 'DRUG CODE', 'NUMBER OF REGISTRANT SOLD TO', 'TOTAL GRAMS', 'AVG GRAMS'],
               ['QUARTER 1', 'QUARTER 2', 'TOTAL GRAMS'],
               ['1ST QUARTER', '2ND QUARTER', 'TOTAL GRAMS']]

businessactivities = ['A - PHARMACIES',
                      'B - HOSPITALS',
                      'C - PRACTITIONERS',
                      'D - TEACHING INSTITUTIONS',
                      'M - MID-LEVEL PRACTITIONERS',
                      'N-U NARCOTIC TREATMENT PROGRAMS',
                      'NTP - NARCOTIC TREATMENT PROGRAMS']

geounitlist = ['REGISTRANT ZIP CODE 3', 'ZIP CODE', 'STATE NAME', 'STATE']

column_titles = ['GEO_UNIT', 'REPORT', 'RUN_DATE', 'REPORT_PD', 'DRUG_CODE',
                 'DRUG_NAME', 'STATE', 'POP_YR', 'TITLE', 'BUSINESS ACTIVITY',
                 'PAGE']

headervar_mapping = {'ARCOS 3 - REPORT': 'REPORT',
                     'ARCOS 2 - REPORT': 'REPORT',
                     'DATE RANGE: REPORT': 'REPORT',
                     'Run Date:': 'RUN_DATE',
                     'DATE:': 'RUN_DATE',
                     'Population Year:': 'POP_YR',
                     'REPORTING PERIOD:': 'REPORT_PD',
                     'DATE RANGE:': 'REPORT_PD',
                     'DRUG CODE:': 'DRUG_CODE',
                     'DRUG NAME:': 'DRUG_NAME',
                     'DRUG: ': 'DRUG_CODE',
                     'STATE:': 'STATE',
                     'BUSINESS ACTIVITY:': 'BUSINESS ACTIVITY',
                     'PAGE:': 'PAGE'}

col_rename_dict = {'NUMBER OF REGISTRANT SOLD TO': 'BUYERS',
                   'TOTAL GRAMS/100K POPULATION': 'GRAMS/100K POP',
                   'STATE POPULATION': '2010 POP', 'STATE NAME': 'STATE'}

american_territories = ['GUAM', 'VIRGIN ISLANDS',
                        'AMERICAN SAMOA', 'PUERTO RICO']

report_sortvars = {'1': ['STATE', 'ZIP3', 'DRUG_CODE', 'DRUG_NAME', 'YEAR',
                         'QUARTER'],
                   '2': ['STATE', 'DRUG_CODE', 'DRUG_NAME', 'YEAR', 'QUARTER'],
                   '3': ['STATE', 'DRUG_CODE', 'DRUG_NAME', 'YEAR', 'QUARTER'],
                   '4': ['STATE', 'YEAR'],
                   '5': ['STATE', 'BUSINESS_ACTIVITY',
                         'DRUG_CODE', 'DRUG_NAME', 'YEAR'],
                   '7': ['BUSINESS_ACTIVITY', 'DRUG_CODE', 'DRUG_NAME',
                         'YEAR'],
                   }

grams_name_dict = {'1': 'GRAMS', '2': 'GRAMS', '3': 'GRAMS_PC',
                   '5': 'TOTAL_GRAMS', '7': 'TOTAL_GRAMS'}

mme = {'BUPRENORPHINE': 40,
       'BUTORPHANOL': 7,
       'CODEINE': .1,
       'DIHYDROCODEINE':  .2,
       'FENTANYL':  75,
       'FENTANYL BASE': 75,
       'HYDROCODONE': 1,
       'HYDROMORPHONE': 4,
       'LEVORPHANOL': 8,
       'MEPERIDINE':  .3333333,
       'MEPERIDINE (PETHIDINE)':  .3333333,
       'METHADONE': 8,
       'MORPHINE':  1,
       'NALBUPHINE':  1,
       'OPIUM': 1,
       'OPIUM POWDERED':  .1,
       'OXYCODONE': 1.5,
       'OXYMORPHONE': 7,
       'PENTAZOCINE': 1,
       'PROPOXYPHENE':  .0634615,
       'SUFENTANIL':  750,
       'TAPENTADOL':  .1,
       'TRAMADOL':  .1}

mat = ['BUPRENORPHINE', 'METHADONE']
opioids_main = ['CODEINE', 'FENTANYL BASE',
                'HYDROCODONE', 'OXYCODONE',
                'MEPERIDINE (PETHIDINE)',
                'HYDROMORPHONE', 'MORPHINE']


zip3_state_crosswalk_file = os.path.join(os.path.dirname(__file__),
                                         'zip3<->state.dta')


def statedf():
    df = state_identifiers()
    df2 = pd.DataFrame({'state': statelist})
    df = df2.merge(df, how='left')
    df.loc[df.state == "GUAM", 'stateabbrev'] = "GU"
    df.loc[df.state == "TRUST TERRITORIES (GUAM)", 'stateabbrev'] = "GU"
    df.loc[df.state == "AMERICAN SAMOA", 'stateabbrev'] = "AS"
    df.loc[df.state == "VIRGIN ISLANDS", 'stateabbrev'] = "VI"
    return df


def statelist_2018_2019():
    df = statedf()
    df['statecodearcos'] = df.stateabbrev + " - " + df.state
    return df.set_index('statecodearcos')['state'].to_dict()


def totallist_2018_2019():
    df = statedf()
    df['statetotal'] = df.stateabbrev + " - " + df.state + " - Total"
    return df.statetotal.values.tolist()


statetotallist = (totallist + statelist + totallist_2018_2019()
                  + ['AS - AMERICAN SAMOA - Tota',
                     'DC - DISTRICT OF COLUMBIA -'])
