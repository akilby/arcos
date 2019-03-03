import os

list_of_titles = ['RETAIL DRUG DISTRIBUTION BY ZIP CODE FOR EACH STATE',
                  'RETAIL DRUG DISTRIBUTION BY ZIP CODE WITHIN STATE BY GRAMS WT',
                  'RETAIL DRUG DISTRIBUTION BY ZIP CODE WITHIN STATE BY GRAMS WEIGHT',
                  'RETAIL DRUG DISTRIBUTION BY DRUG CODE - TOTAL U.S. (IN GRAMS)',
                  'RETAIL DRUG DISTRIBUTION BY STATE WITHIN DRUG CODE BY GRAMS WT',
                  'RETAIL DRUG DISTRIBUTION BY STATE WITHIN DRUG CODE BY GRAMS WEIGHT',
                  'RETAIL DRUG DISTRIBUTION BY DRUG CODE – TOTAL U.S. (IN GRAMS)',
                  'QUARTERLY DRUG DISTRIBUTION BY STATE PER 100K POPULATION BY GRAM WT',
                  'QUARTERLY DRUG DISTRIBUTION BY STATE PER 100,000 POPULATION BY GRAMS WEIGHT',
                  'QUARTERLY DRUG DISTRIBUTION BY STATE / 100K POPULATION BY MILIGRAM WT',
                  'CUMULATIVE DISTRIBUTION BY STATE IN GRAMS PER 100,000 POPULATION',
                  'CUMULATIVE DISTRIBUTION IN GRAMS PER 100,000 POPULATION',
                  'QUARTERLY DISTRIBUTION IN GRAMS PER 100,000 POPULATION',
                  'STATISTICAL SUMMARY FOR RETAIL DRUG PURCHASES BY GRAMS WT',
                  'STATISTICAL SUMMARY FOR RETAIL DRUG PURCHASES BY GRAMS WEIGHT',
                  'STATISTICAL SUMMARY FOR RETAIL DRUG PURCHASES',
                  'UNITED STATES SUMMARY FOR RETAIL DRUG PURCHASES BY GRAMS WT',
                  'UNITED STATES SUMMARY FOR RETAIL DRUG PURCHASES BY GRAMS WEIGHT',
                  'U.S. SUMMARY OF RETAIL DRUG PURCHASES']


totallist = ['UNITED STATES TOTAL',
             'UNITED STATES',
             'U.S. GRAMS / PER 100K POPULATION',
             'U.S. GRAMS / PER 100K:',
             'U.S. GRAMS/100K POP:',
             'U.S. TOTAL',
             'TOTAL',
             'STATE TOTAL',
             'DRUG TOTAL']

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
               ['RANK', 'STATE', '2000 POP', 'TOTAL GRAMS', 'GRAMS/100K POP'],
               ['RANK', 'STATE', '2010 POP', 'TOTAL GRAMS', 'GRAMS/100K POP'],
               ['DRUG NAME', 'DRUG CODE', 'BUYERS', 'TOTAL GRAMS', 'AVG GRAMS']]

businessactivities = ['A - PHARMACIES',
                      'B - HOSPITALS',
                      'C - PRACTITIONERS',
                      'D - TEACHING INSTITUTIONS',
                      'M - MID-LEVEL PRACTITIONERS',
                      'N-U NARCOTIC TREATMENT PROGRAMS']

geounitlist = ['ZIP CODE', 'STATE']

column_titles = ['GEO_UNIT', 'REPORT', 'RUN_DATE', 'REPORT_PD', 'DRUG_CODE',
                 'DRUG_NAME', 'STATE', 'POP_YR', 'TITLE', 'BUSINESS ACTIVITY',
                 'PAGE']

headervar_mapping = {'ARCOS 3 - REPORT': 'REPORT',
                     'ARCOS 2 - REPORT': 'REPORT',
                     'Run Date:': 'RUN_DATE',
                     'DATE:': 'RUN_DATE',
                     'Population Year:': 'POP_YR',
                     'REPORTING PERIOD:': 'REPORT_PD',
                     'DATE RANGE:': 'REPORT_PD',
                     'DRUG CODE:': 'DRUG_CODE',
                     'DRUG NAME:': 'DRUG_NAME',
                     'STATE:': 'STATE',
                     'BUSINESS ACTIVITY:': 'BUSINESS ACTIVITY',
                     'PAGE:': 'PAGE'}

statetotallist = totallist + statelist
zip3_state_crosswalk_file = os.path.join(os.path.dirname(__file__),
                                         'zip3<->state.dta')
