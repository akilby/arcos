import os
import pandas as pd
import numpy as np

from .data.data import statelist, zip3_state_crosswalk_file


def save_to_disk(Reports_dict, folder):
    if not os.path.isdir(folder):
        os.mkdir(folder)
    report_final_dict = {}
    drugset = set()
    list_of_reports = []
    for rept in ['1', '2', '3', '4', '5', '7']:
        if Reports_dict[rept].shape != (0, 0):
            list_of_reports.append(rept)

    for reportno in list_of_reports:

        rpt_final = pd.DataFrame()
        rpt = Reports_dict[reportno][sorted(list(set(Reports_dict[reportno].columns) - {'PAGE', 'TOTAL', 'AVG GRAMS'}))].copy()

        rpt = rpt.rename(columns={'DRUG CODE': 'DRUG_CODE', 'DRUG NAME': 'DRUG_NAME'})

        if 'DRUG_CODE' in rpt.columns:
            rpt['DRUG_NAME'] = rpt[['DRUG_NAME', 'DRUG_CODE']].apply(lambda x: lambda_to_fix_drugname(x), axis=1)
            rpt['DRUG_CODE'] = rpt[['DRUG_NAME', 'DRUG_CODE']].apply(lambda x: lambda_to_fix_drugcode(x), axis=1)

        if reportno == '1' or reportno == '2' or reportno == '3':

            if reportno == '1' or reportno == '2':
                grams_title = 'GRAMS'
            if reportno == '3':
                grams_title = 'GRAMS_PC'

            if reportno == '1':
                assert set(rpt['STATE']).issubset(set(statelist))
                remove_list = [x for x in set(rpt['GEO']) if x.isdigit() is False]
            if reportno == '2' or reportno == '3':
                remove_list = [x for x in set(rpt['GEO']) if x not in statelist]
            for tot in remove_list:
                rpt = rpt.loc[rpt['GEO'] != tot]

            if reportno == '1':
                assert [x for x in set(rpt['GEO']) if x.isdigit() is False] == []
                rpt['GEO'] = rpt['GEO'].apply(lambda x: '%03d' % int(x))
                assert rpt[rpt.apply(lambda x: len(x['GEO']) != 3, axis=1)].shape[0] == 0
            if reportno == '2' or reportno == '3':
                assert [x for x in set(rpt['GEO']) if x not in statelist] == []

            for col in ['Q1', 'Q2', 'Q3', 'Q4']:
                rpt[col] = rpt[col].apply(lambda x: float(str(x).replace(',', '')))

            if reportno == '3':
                for col in ['Q1', 'Q2', 'Q3', 'Q4']:
                    rpt[col][rpt['YEAR'] == '2011'] = rpt[col] / 1000

            usecols = [x for x in rpt.columns if not x.startswith('Q')]
            rpt_final = pd.DataFrame()
            for num in range(1, 5):
                rpt_q = rpt[usecols + ['Q%s' % num]]
                rpt_q.columns = usecols + [grams_title]
                rpt_q['Q'] = num
                rpt_final = pd.concat([rpt_final, rpt_q])

            if reportno == '1':
                rpt_final = rpt_final.rename(columns={'GEO': 'ZIP'})
            if reportno == '2' or reportno == '3':
                rpt_final = rpt_final.rename(columns={'GEO': 'STATE'})

        if reportno == '4':
            rpt['POP'] = np.nan
            if '2000 POP' in list(rpt.columns):
                rpt['POP'][rpt['2000 POP'].notnull()] = rpt['2000 POP']
            if '2010 POP' in list(rpt.columns):
                rpt['POP'][rpt['2010 POP'].notnull()] = rpt['2010 POP']
            rpt_final = pd.DataFrame([(float(str(x[0]).replace(',', '')), x[1], x[2]) for x in list(set([tuple(x) for x in rpt[['POP', 'YEAR', 'STATE']].values]))])
            rpt_final.columns = ['POP', 'YEAR', 'STATE']
            remove_list = [x for x in set(rpt_final['STATE']) if x not in statelist]
            for tot in remove_list:
                rpt_final = rpt_final.loc[rpt_final['STATE'] != tot]
            rpt_final = rpt_final.sort_values(['STATE', 'YEAR']).reset_index(drop=True)

        if reportno == '5' or reportno == '7':
            if reportno == '5':
                set(rpt['STATE']).issubset(set(statelist))

            rpt['BUSINESS ACTIVITY'] = rpt['BUSINESS ACTIVITY'].apply(lambda x: x.split(' - ')[-1:][0])
            rpt['BUYERS'] = rpt['BUYERS'].apply(lambda x: int(str(x).replace(',', '')))
            rpt['TOTAL GRAMS'] = rpt['TOTAL GRAMS'].apply(lambda x: float(str(x).replace(',', '')))
            rpt_final = rpt.copy()

        if reportno != '4':
            drugset = drugset | set([tuple(x) for x in rpt_final[['DRUG_CODE', 'DRUG_NAME']].values])
        report_final_dict[reportno] = rpt_final

    drugset_dict = {}
    for item in sorted(drugset):
        if item[0] not in drugset_dict and item[0] != '':
            drugset_dict[item[0]] = item[1]

    for item in [x[0] for x in drugset if x[0] not in drugset_dict.keys()]:
        drugset_dict[item] = ''

    for reportno in list_of_reports:

        rpt = pd.DataFrame()
        rpt = report_final_dict[reportno].copy()

        if 'DRUG_CODE' in rpt.columns:
            rpt['DRUG_NAME'] = rpt[['DRUG_NAME', 'DRUG_CODE']].apply(lambda x: lambda_to_replace_drugname(x, drugset_dict), axis=1)
            rpt['DRUG_CODE'] = rpt['DRUG_CODE'].apply(lambda x: str(x))

        if reportno != '7':
            rpt['STATE'] = rpt['STATE'].apply(lambda x: lambda_guam(x))

        if reportno == '1':
            rpt['GRAMS'] = rpt.apply(lambda x: lambda_for_hand_fix(x, reportno), axis=1)
            rpt = get_rid_of_bad_zips(rpt, zip3_state_crosswalk_file)
            rpt['QUARTER'] = pd.to_datetime(rpt['YEAR'].astype(str) + 'Q' + rpt['Q'].astype(str))
            rpt = rpt.rename(columns={'ZIP': 'ZIP3'})
            rpt = rpt[[x for x in rpt.columns if x not in ['Q']]]
            rpt = rpt.groupby(['DRUG_CODE', 'DRUG_NAME', 'STATE', 'YEAR', 'ZIP3', 'QUARTER']).sum()

        if reportno == '2':
            rpt['GRAMS'] = rpt.apply(lambda x: lambda_for_hand_fix(x, reportno), axis=1)
            rpt['QUARTER'] = pd.to_datetime(rpt['YEAR'].astype(str) + 'Q' + rpt['Q'].astype(str))
            rpt = rpt[[x for x in rpt.columns if x not in ['Q']]]
            rpt = rpt.groupby(['DRUG_CODE', 'DRUG_NAME', 'STATE', 'YEAR', 'QUARTER']).sum()

        if reportno == '3':
            rpt['GRAMS_PC'] = rpt.apply(lambda x: lambda_for_hand_fix(x, reportno), axis=1)
            rpt['QUARTER'] = pd.to_datetime(rpt['YEAR'].astype(str) + 'Q' + rpt['Q'].astype(str))
            rpt = rpt[[x for x in rpt.columns if x not in ['Q']]]
            rpt = rpt.groupby(['DRUG_CODE', 'DRUG_NAME', 'STATE', 'YEAR', 'QUARTER']).sum()

        if reportno == '4':
            rpt = rpt.groupby(['STATE', 'YEAR']).sum()

        if reportno == '5':
            rpt['TOTAL GRAMS'] = rpt.apply(lambda x: lambda_for_hand_fix(x, reportno), axis=1)
            rpt = rpt.groupby(['DRUG_CODE', 'DRUG_NAME', 'STATE', 'YEAR', 'BUSINESS ACTIVITY']).sum()

        if reportno == '7':
            rpt['TOTAL GRAMS'] = rpt.apply(lambda x: lambda_for_hand_fix(x, reportno), axis=1)
            rpt = rpt.groupby(['DRUG_CODE', 'DRUG_NAME', 'YEAR', 'BUSINESS ACTIVITY']).sum()

        rpt.to_stata(os.path.join(folder, 'Report%s.dta' % reportno))
        report_final_dict[reportno] = rpt

    return report_final_dict, drugset


def get_rid_of_bad_zips(rpt, zip3_state_crosswalk_file):
    zip_state = pd.read_stata(zip3_state_crosswalk_file)
    r = rpt.groupby(['DRUG_CODE', 'DRUG_NAME', 'STATE', 'YEAR', 'ZIP']).sum()
    u = r.reset_index()
    r = r.groupby(['STATE', 'ZIP']).sum()['GRAMS']
    p = r.groupby(['ZIP']).sum()
    q = pd.merge(pd.DataFrame(r), pd.DataFrame(p), how='outer', left_index=True, right_index=True)
    s = pd.merge(q.reset_index(), zip_state, left_on=['STATE', 'ZIP'], right_on=['state', 'zip3'], how='left')
    s['perc'] = s['GRAMS_x'] / s['GRAMS_y']
    s['max'] = s.groupby(['ZIP'])['perc'].transform(max) == s['perc']
    t = s[s.apply(lambda df: df['max'] is True and df['state'] == df['STATE'], axis=1)]
    dropped_zips = set(u[u['STATE'].apply(lambda x: x in list(set(zip_state['state'])))]['ZIP']) - set(t['ZIP'])
    assert dropped_zips.issubset(["962", "965", "091", "345", "353", "702"])
    assert t[['STATE', 'ZIP']].drop_duplicates().equals(t[['STATE', 'ZIP']])
    v = pd.merge(rpt, t[['STATE', 'ZIP']], on=['STATE', 'ZIP'], how='right')
    return v


def lambda_guam(x):
    if 'GUAM' in x:
        return 'GUAM'
    else:
        return x


def lambda_to_fix_drugname(df):
    if 'DRUGNAME:' in df['DRUG_CODE']:
        return df['DRUG_CODE'].split('DRUGNAME:')[1].strip()
    return df['DRUG_NAME']


def lambda_to_fix_drugcode(df):
    if 'DRUGNAME:' in df['DRUG_CODE']:
        drugcode = df['DRUG_CODE'].split('DRUGNAME:')[0].strip()
    else:
        drugcode = df['DRUG_CODE']
    if drugcode == '9041L':
        drugcode = '9041'
    return drugcode


def lambda_to_replace_drugname(df, drugset_dict):
    return drugset_dict[df['DRUG_CODE']]


def lambda_for_hand_fix(df, reportno):

    if reportno in ['1', '2']:
        grams = df['GRAMS']

    if reportno in ['3']:
        grams = df['GRAMS_PC']

    if reportno in ['5', '7']:
        grams = df['TOTAL GRAMS']

    if reportno == '1' and df['ZIP'] == '297' and df['STATE'] == "SOUTH CAROLINA" and df['YEAR'] == '2013' and df['Q'] == 1 and df['DRUG_NAME'] == 'FENTANYL BASE' and df['GRAMS'] > 78000:
        grams = 89.48750305175781
        print('report %s, altering fentanyl grams' % reportno)

    if reportno == '1' and df['ZIP'] == '571' and df['STATE'] == "SOUTH DAKOTA" and df['YEAR'] == '2011' and df['Q'] == 2 and df['DRUG_NAME'] == 'FENTANYL BASE' and df['GRAMS'] > 1400:
        grams = 217.6462554931641
        print('report %s, altering fentanyl grams' % reportno)

    if reportno == '1' and df['ZIP'] == '837' and df['STATE'] == "IDAHO" and df['YEAR'] == '2017' and df['Q'] == 1 and df['DRUG_NAME'] == 'OXYCODONE' and df['GRAMS'] > 100000:
        grams = 13671.485
        print('report %s, altering Idaho oxycodone grams for 837' % reportno)

    if reportno == '2' and df['STATE'] == "SOUTH CAROLINA" and df['YEAR'] == '2013' and df['Q'] == 1 and df['DRUG_NAME'] == 'FENTANYL BASE' and df['GRAMS'] > 78000:
        grams = 1632.327522277832
        print('report %s, altering fentanyl grams' % reportno)

    if reportno == '2' and df['STATE'] == "SOUTH DAKOTA" and df['YEAR'] == '2011' and df['Q'] == 2 and df['DRUG_NAME'] == 'FENTANYL BASE' and df['GRAMS'] > 1400:
        grams = 443.3562550544739
        print('report %s, altering fentanyl grams' % reportno)

    if reportno == '3' and df['STATE'] == "SOUTH CAROLINA" and df['YEAR'] == '2013' and df['Q'] == 1 and df['DRUG_NAME'] == 'FENTANYL BASE':
        grams = 100000 * 1632.327522277832 / 4625364
        print('report %s, altering fentanyl grams' % reportno)

    if reportno == '3' and df['STATE'] == "SOUTH DAKOTA" and df['YEAR'] == '2011' and df['Q'] == 2 and df['DRUG_NAME'] == 'FENTANYL BASE':
        grams = 100000 * 443.3562550544739 / 814180
        print('report %s, altering fentanyl grams' % reportno)

    if reportno == '5' and df['STATE'] == "SOUTH CAROLINA" and df['YEAR'] == '2013' and df['DRUG_NAME'] == 'FENTANYL BASE' and df['BUSINESS ACTIVITY'] == 'HOSPITALS':
        grams = df['TOTAL GRAMS'] - (78209.6484375 - 89.48750305175781)
        print('report %s, altering fentanyl grams' % reportno)

    if reportno == '5' and df['STATE'] == "SOUTH DAKOTA" and df['YEAR'] == '2011' and df['DRUG_NAME'] == 'FENTANYL BASE' and df['BUSINESS ACTIVITY'] == 'HOSPITALS':
        grams = df['TOTAL GRAMS'] - (14240.349609375 - 217.6462554931641)
        print('report %s, altering fentanyl grams' % reportno)

    if reportno == '7' and df['YEAR'] == '2013' and df['DRUG_NAME'] == 'FENTANYL BASE' and df['BUSINESS ACTIVITY'] == 'HOSPITALS':
        grams = df['TOTAL GRAMS'] - (78209.6484375 - 89.48750305175781)
        print('report %s, altering fentanyl grams' % reportno)

    if reportno == '7' and df['YEAR'] == '2011' and df['DRUG_NAME'] == 'FENTANYL BASE' and df['BUSINESS ACTIVITY'] == 'HOSPITALS':
        grams = df['TOTAL GRAMS'] - (14240.349609375 - 217.6462554931641)
        print('report %s, altering fentanyl grams' % reportno)

    return grams


def subset_to_useful_columns(df):
    unique_cols = []
    manyval_cols = []
    empty_cols = []
    for col in list(df.columns):
        if set(df[col].tolist()) == {''}:
                empty_cols.append(col)
        elif len(set(df[col].tolist())) == 1:
                unique_cols.append(col)
        else:
                manyval_cols.append(col)
    return(df[manyval_cols])


def update_Reports_dict(Report_dict, Reports_dict):
    for key in Report_dict:
        updated = pd.concat([Reports_dict[key], Report_dict[key]])
        updated = updated.reset_index(drop=True)
        Reports_dict[key] = updated
    return Reports_dict