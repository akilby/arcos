import os
from functools import reduce

import numpy as np
import pandas as pd
from arcos.data.data import (american_territories, grams_name_dict, mat, mme,
                             opioids_main, report_sortvars, statelist,
                             zip3_state_crosswalk_file)


def final_clean(Reports_dict, folder, cachedir, print_outlier_threshold=6):
    """
    note: even if include_territories is set to true, wont
    have them in report 1
    """
    from arcos.build import pickle_dump
    if not os.path.isdir(folder):
        os.mkdir(folder)

    report_final_dict = {}

    geo_name_dict = {'1': 'ZIP', '2': 'STATE', '3': 'STATE'}
    timevars = ['YEAR', 'QUARTER']

    list_of_reports = [key for key, val in Reports_dict.items()
                       if val.shape != (0, 0)]

    for reportno in list_of_reports:
        rpt = (Reports_dict[reportno]
               .pipe(process_cols)
               .pipe(split_off_drug_code_from_drug_name)
               .pipe(remove_total_lines, reportno)
               .pipe(fix_quarter_quantity_columns, reportno)
               .pipe(reshape_long, reportno, grams_name_dict, geo_name_dict)
               .pipe(report_4_to_populations, reportno)
               .pipe(business_activity_fixes, reportno)
               )
        report_final_dict[reportno] = rpt

    report_final_dict = clean_drugs(report_final_dict, grams_name_dict)
    report_final_dict = clean_states(report_final_dict, statelist)
    report_final_dict = clean_zips(report_final_dict)
    report_final_dict = clean_business_activities(report_final_dict)
    report_final_dict = clean_time_vars(report_final_dict, report_sortvars)

    checks = run_consistency_checks(report_final_dict)

    problem_string = 'YEAR=="2012"'
    if identify_problem_report(checks, problem_string) == {2}:
        report_final_dict = fix_2_from_other_reports(
            report_final_dict, checks, problem_string)

    checks = run_consistency_checks(report_final_dict)
    assert identify_problem_report(checks, problem_string) == {}

    problem_string = 'YEAR=="2011"'
    if identify_problem_report(checks, problem_string) == {7}:
        report_final_dict = fix_7_from_other_reports(
            report_final_dict, checks, problem_string)

    checks = run_consistency_checks(report_final_dict)
    assert identify_problem_report(checks, problem_string) == {}

    print('Final conclusion: Able to get everything to a near '
          'match except about 16 entries for Report 1, '
          'which cant be fixed because r1 is most granular '
          'geography. It is a slight undercount in some cases '
          'relative to state reports except for Massachusetts '
          'PENTOBARBITAL (SCHEDULE 2) in 2017, which is missing '
          'about 10 percent of the state count')

    if (1, 2) in checks:
        print(checks[(1, 2)].query('GRAMS_x_GRAMS_y_ratio_check==False'))

    report_final_dict = generate_outlier_indicators(
        report_final_dict, timevars, print_outlier_threshold)
    report_final_dict = add_drug_info_variables(report_final_dict)

    pickle_dump(report_final_dict,
                os.path.join(cachedir, 'report_final_dict.pkl'))

    {rpt.to_stata(os.path.join(folder, 'Report%s.dta' % reportno),
                  convert_dates={'QUARTER': 'tq' for x in rpt.columns
                                 if x == 'QUARTER'},
                  write_index=False)
     for reportno, rpt in report_final_dict.items()}

    return report_final_dict


# problem_string = ('(DRUG_NAME=="HYDROCODONE" '
#                   'or DRUG_NAME=="PENTOBARBITAL (SCHEDULE 2)" '
#                   'or DRUG_NAME=="METHYLPHENIDATE" '
#                   'or DRUG_NAME== "HYDROMORPHONE" '
#                   'or DRUG_NAME== "FENTANYL BASE"'
#                   'or DRUG_NAME=="SUFENTANIL") '
#                   'and YEAR=="2012"')


def add_drug_info_variables(report_final_dict):
    drugset = make_sanitized_list_of_drugs(report_final_dict)
    drugset['OPIOIDS_PRIMARY'] = np.where(
        drugset.DRUG_NAME.isin(opioids_main), True, False)
    drugset['MAT'] = np.where(drugset.DRUG_NAME.isin(mat), True, False)
    drugset = drugset.merge(pd.DataFrame
                              .from_dict(mme, orient='index')
                              .reset_index()
                              .rename(columns={0: 'MME',
                                               'index': 'DRUG_NAME'}),
                            how='left')
    return {key: val.merge(drugset, how='left') if key != '4' else val
            for key, val in report_final_dict.items()}


def identify_problem_report(checks, qstring):
    list_of_mismatches, list_of_matches = [], []
    for key, val in checks.items():
        qdf = val.query(qstring)
        perc = qdf[[x for x in val.columns
                    if x.endswith('_ratio_check')
                    and 'GRAMS' in x]].sum()/qdf.shape[0]
        print(key, ' : ', perc)
        if perc.values[0] < 1:
            list_of_mismatches.append(key)
        else:
            list_of_matches.append(key)
    if not list_of_mismatches:
        return {}
    problem_report = set.intersection(*[set(x) for x in list_of_mismatches])
    if not all([~problem_report.issubset(set(x)) for x in list_of_matches]):
        problem_report = set()
    assert all([problem_report.issubset(x) for x in list_of_mismatches])
    return problem_report


def fix_2_from_other_reports(report_final_dict, checks, qstring):
    df = (checks[(1, 2)]
          .drop(columns='GRAMS_y')
          .query(qstring)
          .merge(checks[(2, 3, 4)]
                 .query(qstring)
                 .drop(columns=['GRAMS_x', 'GRAMS_PC_x']),
                 how='outer',
                 left_index=True,
                 right_index=True)[['GRAMS_x', 'GRAMS_PC_y', 'POP']]
          .assign(GRAMS_y=lambda x: x.POP * x.GRAMS_PC_y/100000))
    # Use report 1 to fix unless it's missing and report 3 is large enough
    df['GRAMS'] = np.where(df.GRAMS_x.isnull() & (df.GRAMS_PC_y >= 2),
                           df.GRAMS_y, df.GRAMS_x)
    # checks per capita data doesn't bring any contradictory info
    # when can get from r1 and r3
    assert all((df.GRAMS == df.GRAMS_y)
               | (abs(df.GRAMS/df.GRAMS_y - 1) < 0.01)
               | (df.GRAMS_PC_y <= 2))
    df = df['GRAMS'].reset_index()
    assert df.shape == report_final_dict['2'].query(qstring).shape
    r2 = report_final_dict['2'].merge(
        df.rename(columns={'GRAMS': 'GRAMS_r'}), how='left')
    r2['GRAMS'] = np.where(r2.GRAMS_r.notnull(), r2.GRAMS_r, r2.GRAMS)
    report_final_dict['2'] = r2.drop(columns='GRAMS_r')
    return report_final_dict


def fix_7_from_other_reports(report_final_dict, checks, qstring):
    df = (checks[(5, 7)]
          .query(qstring))[['BUYERS_x', 'TOTAL_GRAMS_x']]
    r7 = report_final_dict['7'].merge(df.reset_index(), how='outer')
    r7['BUYERS'] = np.where(r7.BUYERS_x.notnull(), r7.BUYERS_x, r7.BUYERS)
    r7['TOTAL_GRAMS'] = np.where(
        r7.TOTAL_GRAMS_x.notnull(), r7.TOTAL_GRAMS_x, r7.TOTAL_GRAMS)
    r7 = (r7.query('not (DRUG_CODE=="7439" and YEAR=="2011")')
            .reset_index(drop=True))
    report_final_dict['7'] = r7.drop(columns=['BUYERS_x', 'TOTAL_GRAMS_x'])
    return report_final_dict


def run_consistency_checks(report_final_dict, check=[1, 2, 3, 5, 7]):
    if isinstance(check, int):
        check = [check]
    if isinstance(check, str):
        check = [int(check)]
    checks = {}
    if ((1 in check or 2 in check)
            and set(['1', '2']).issubset(report_final_dict)):
        print('\n--- checking 1 against 2 ---')
        checks[(1, 2)] = consistency_check(report_final_dict['1'],
                                           report_final_dict['2'],
                                           grams_name_dict)
    if ((1 in check or 3 in check)
            and set(['1', '3', '4']).issubset(report_final_dict)):
        print('\n--- checking 1 against 3, using 4 ---')
        checks[(1, 3, 4)] = consistency_check(report_final_dict['1'],
                                              report_final_dict['3'],
                                              grams_name_dict,
                                              report_final_dict['4'])
    if ((1 in check or 5 in check)
            and set(['1', '5']).issubset(report_final_dict)):
        print('\n--- checking 1 against 5 ---')
        checks[(1, 5)] = consistency_check(report_final_dict['1'],
                                           report_final_dict['5'],
                                           grams_name_dict)
    if ((2 in check or 3 in check)
            and set(['2', '3', '4']).issubset(report_final_dict)):
        print('\n--- checking 2 against 3, using 4 ---')
        checks[(2, 3, 4)] = consistency_check(report_final_dict['2'],
                                              report_final_dict['3'],
                                              grams_name_dict,
                                              report_final_dict['4'])
    if ((2 in check or 5 in check)
            and set(['2', '5']).issubset(report_final_dict)):
        print('\n--- checking 2 against 5 ---')
        checks[(2, 5)] = consistency_check(report_final_dict['2'],
                                           report_final_dict['5'],
                                           grams_name_dict)
    if ((2 in check or 7 in check)
            and set(['2', '7']).issubset(report_final_dict)):
        print('\n--- checking 2 against 7 ---')
        checks[(2, 7)] = consistency_check(report_final_dict['2'],
                                           report_final_dict['7'],
                                           grams_name_dict)
    if ((3 in check or 5 in check)
            and set(['3', '5', '4']).issubset(report_final_dict)):
        print('\n--- checking 3 against 5, using 4 ---')
        checks[(3, 5)] = consistency_check(report_final_dict['3'],
                                           report_final_dict['5'],
                                           grams_name_dict,
                                           report_final_dict['4'])
    if ((3 in check or 7 in check)
            and set(['3', '7', '4']).issubset(report_final_dict)):
        print('\n--- checking 3 against 7, using 4 ---')
        checks[(3, 7)] = consistency_check(report_final_dict['3'],
                                           report_final_dict['7'],
                                           grams_name_dict,
                                           report_final_dict['4'])
    if ((5 in check or 7 in check)
            and set(['5', '7']).issubset(report_final_dict)):
        print('\n--- checking 5 against 7 ---')
        checks[(5, 7)] = consistency_check(report_final_dict['5'],
                                           report_final_dict['7'],
                                           grams_name_dict)
    return checks


def consistency_check(r1, r2, grams_name_dict,
                      pop_df=pd.DataFrame(),
                      how='outer'):
    if 'ZIP3' in r1.columns:
        r2 = (r2.merge(pd.DataFrame({'STATE': american_territories}),
                       how='left', indicator=True)
                .query('_merge=="left_only"')
                .drop(columns='_merge'))
    if 'ZIP3' in r2.columns:
        r1 = (r1.merge(pd.DataFrame({'STATE': american_territories}),
                       how='left', indicator=True)
                .query('_merge=="left_only"')
                .drop(columns='_merge'))
    if ('ZIP3' in r1.columns or 'ZIP3' in r2.columns) and not pop_df.empty:
        pop_df = (pop_df.merge(pd.DataFrame({'STATE': american_territories}),
                               how='left', indicator=True)
                        .query('_merge=="left_only"')
                        .drop(columns='_merge'))

    name1 = [x for x in r1.columns if x in list(grams_name_dict.values())]
    name2 = [x for x in r2.columns if x in list(grams_name_dict.values())]
    assert len(name1) == 1 and len(name2) == 1

    name1 = name1[0]
    name2 = name2[0]
    outlier_cols = ['OUTLIER_SCORE', 'OUTLIER_REPLACE']
    if not pop_df.empty and (name1.endswith('_PC')):
        r1 = r1.merge(pop_df)
        r1[f'{name2}_x'] = r1.GRAMS_PC * r1.POP/100000
    if not pop_df.empty and (name2.endswith('_PC')):
        r2 = r2.merge(pop_df)
        r2[f'{name1}_y'] = r2.GRAMS_PC * r2.POP/100000

    check1 = (r1.drop(columns=[x for x in r1.columns if x in outlier_cols])
                .groupby([x for x in r1.columns if x in r2.columns
                         and x not in outlier_cols
                         + list(grams_name_dict.values()) + ['BUYERS']])
                .sum())
    if r1.shape[0] != check1.shape[0] and 'GRAMS_PC' in check1.columns:
        check1 = check1.drop(columns=['POP', 'GRAMS_PC'])
        check1 = check1.merge(pop_df.groupby(
            [x for x in pop_df.columns
             if x in list(check1.index.names)]).sum(),
            left_index=True, right_index=True)
        check1 = check1.assign(GRAMS_PC=100000*check1[f'{name2}_x']/check1.POP)
        # name1 = 'GRAMS_x'
    check2 = (r2.drop(columns=[x for x in r2.columns if x in outlier_cols])
                .groupby([x for x in r1.columns if x in r2.columns
                         and x not in outlier_cols
                         + list(grams_name_dict.values()) + ['BUYERS']])
                .sum())
    if r2.shape[0] != check2.shape[0] and 'GRAMS_PC' in check2.columns:
        check2 = check2.drop(columns=['POP', 'GRAMS_PC'])
        check2 = check2.merge(pop_df.groupby(
            [x for x in pop_df.columns
             if x in list(check2.index.names)]).sum(),
            left_index=True, right_index=True)
        check2 = check2.assign(GRAMS_PC=100000*check2[f'{name1}_x']/check2.POP)
        # name2 = 'GRAMS_y'
    check = check1.merge(
        check2, left_index=True, right_index=True, how=how)
    if name1 == name2:
        name1 = name1 + '_x'
        name2 = name2 + '_y'
    if not pop_df.empty and (name1.endswith('_PC') or name2.endswith('_PC')):
        name_npc = name1 if name2.endswith('_PC') else name2
        name_pc = name2 if name2.endswith('_PC') else name1
        # pop_df = pop_df.groupby(
        #     [x for x in pop_df.columns if x in list(check.index.names)]).sum()
        # check = (check.merge(pop_df, how=how,
        #                      left_index=True, right_index=True)
        #               .assign(**{name_npc + '_':
        #                          lambda df: df.GRAMS_PC*df.POP/100000,
        #                          name_pc + '_':
        #                          lambda df: 100000*df[name_npc]/df.POP})
        #               .rename(columns={name1: '%s_x' % name1,
        #                                name2: '%s_y' % name2}))
        check = (
            check
            .assign(**{name_pc + '_': lambda df: 100000*df[name_npc]/df.POP})
            .rename(columns={name1: '%s_x' % name1, name2: '%s_y' % name2}))
        check = check.rename(
            columns={name1 + '_': (name1 + '_y'
                                   if name1 + '_x' in check.columns
                                   else name1 + '_x'),
                     name2 + '_': (name2 + '_y'
                                   if name2 + '_x' in check.columns
                                   else name2 + '_x')})
        name1, name2 = name_npc + '_x', name_npc + '_y'
        name12, name22 = name_pc + '_x', name_pc + '_y'

        check = check.assign(
            **{'%s_%s_or_%s_%s_check' % (name1, name2, name12, name22):
               (abs(check[name1] - check[name2]) < 0.1)
               | (abs(check[name12] - check[name22]) < 0.1),
               '%s_%s_ratio' % (name1, name2):
                check[name1]/check[name2]})
        check = check.assign(
            **{'%s_%s_ratio_check' % (name1, name2):
                (((check[name1]/check[name2] < 1.01)
                    & (check[name1]/check[name2] > 0.99))
                 | check[f'{name1}_{name2}_or_{name12}_{name22}_check'])
               })
        print('Non-perfect matches for %s and %s or %s and %s'
              % (name1, name2, name12, name22))
        print(check[f'{name1}_{name2}_or_{name12}_{name22}_check']
              .value_counts(dropna=False))
    else:
        check = check.assign(
            **{'%s_%s_check' % (name1, name2):
               abs(check[name1] - check[name2]) < 0.1,
               '%s_%s_ratio' % (name1, name2):
                check[name1]/check[name2],
                '%s_%s_ratio_check' % (name1, name2):
                (((check[name1]/check[name2] < 1.01)
                    & (check[name1]/check[name2] > 0.99))
                 | (abs(check[name1] - check[name2]) < 0.1))
               })
        print('Non-perfect matches for %s and %s' % (name1, name2))
        print(check['%s_%s_check' % (name1, name2)].value_counts(dropna=False))
    print('Non-near matches for %s and %s' % (name1, name2))
    print(check['%s_%s_ratio_check' % (name1, name2)]
          .value_counts(dropna=False))
    if 'BUYERS' in r1.columns and 'BUYERS' in r2.columns:
        check = check.assign(BUYERS_check=check.BUYERS_x == check.BUYERS_y,
                             BUYERS_ratio=check.BUYERS_x/check.BUYERS_y,
                             BUYERS_ratio_check=(
                                ((check.BUYERS_x/check.BUYERS_y < 1.01)
                                 & (check.BUYERS_x/check.BUYERS_y > 0.99))
                                | (abs(check.BUYERS_x - check.BUYERS_y) < 0.1)
                                ))
    return check


def clean_drugs(report_final_dict, grams_name_dict):
    """
    harmonize the drug_code drug_list mapping
    drop drug code 9003 and 9041 because they are only 0s
    """
    drugset = make_sanitized_list_of_drugs(report_final_dict)
    report_final_dict2 = {
     key: val.drop(columns='DRUG_NAME').merge(drugset, on='DRUG_CODE')
     if key != '4' else val for key, val in report_final_dict.items()}
    # Make sure that all 9003 and 9041 entries are actually just zero
    assert (pd.concat(
        [val.query('DRUG_CODE=="9003" and %s!=0' % grams_name_dict[key])
         for key, val in report_final_dict2.items() if key != '4']).shape[0]
            == 0)
    assert (pd.concat(
        [val.query('DRUG_CODE=="9041" and %s!=0' % grams_name_dict[key])
         for key, val in report_final_dict2.items() if key != '4']).shape[0]
            == 0)
    # then drop
    report_final_dict2 = {key:
                          (val
                           .query('DRUG_CODE!="9003" and DRUG_CODE!="9041"')
                           .reset_index(drop=True))
                          if key != '4' else val
                          for key, val in report_final_dict2.items()
                          }
    assert all([all(report_final_dict2[r]['DRUG_CODE']
                    .apply(lambda x: isinstance(x, str)))
                for r in report_final_dict2.keys() if r != '4'])

    drugset = make_sanitized_list_of_drugs(report_final_dict2)
    data_frames = [drugset] + [(val.groupby('DRUG_CODE')
                                   .size()
                                   .rename(f'r{key}')
                                   .reset_index()) for key, val
                               in report_final_dict2.items() if key != '4']
    df_merged = reduce(lambda left, right: pd.merge(left, right, how='outer'),
                       data_frames)
    print('Only appears in one report:')
    print(df_merged[df_merged.isnull().sum(1) == 4])
    report_final_dict2 = {
        key:
        (val.merge(
          df_merged[df_merged.isnull().sum(1) == 4][['DRUG_CODE']],
          how='left', indicator=True)
            .query('_merge=="left_only"')
            .drop(columns='_merge')
            .reset_index(drop=True))
        if key != '4' else val for key, val
        in report_final_dict2.items()}

    return report_final_dict2


def make_sanitized_list_of_drugs(report_final_dict):
    """
    Makes a sanitized drugcode-drugname crosswalk
    """
    list_of_reports = [key for key, val in report_final_dict.items()
                       if val.shape[0] != (0, 0)]
    drugset = (pd.concat([report_final_dict[x][['DRUG_CODE', 'DRUG_NAME']]
                          for x in list_of_reports if x != '4'])
                 .drop_duplicates()
                 .sort_values(['DRUG_CODE', 'DRUG_NAME'])
                 .reset_index(drop=True))
    drugset_dups = (drugset[drugset['DRUG_CODE'].duplicated(keep=False)]
                    .reset_index(drop=True))
    drugset_dups['NEWNAME'] = (drugset_dups
                               .DRUG_NAME
                               .str.split('(').str[0]
                               .str.split('[').str[0]
                               .str.split('DRUG CODE:').str[0]
                               .str.strip())
    drugset_dups = (drugset_dups
                    .query('NEWNAME!=""')[['DRUG_CODE', 'NEWNAME']]
                    .drop_duplicates()
                    .rename(columns={'NEWNAME': 'DRUG_NAME'}))
    drugset = drugset.merge(drugset_dups, on='DRUG_CODE', how='left')
    drugset['DRUG_NAME'] = np.where(drugset.DRUG_NAME_y.notnull(),
                                    drugset.DRUG_NAME_y,
                                    drugset.DRUG_NAME_x)
    drugset['DRUG_NAME'] = np.where(drugset.DRUG_CODE == '7365',
                                    'DRONABINOL IN AN ORAL SOLUTION',
                                    drugset.DRUG_NAME)
    drugset['DRUG_NAME'] = np.where(drugset.DRUG_CODE == '1248',
                                    'MEPHEDRONE; 4-METHOXYMETHCATHINONE',
                                    drugset.DRUG_NAME)
    drugset['DRUG_NAME'] = np.where(drugset.DRUG_CODE == '7439',
                                    '5-METHOXY-N,N-DIISOPROPYLTRYPTAMINE',
                                    drugset.DRUG_NAME)
    drugset['DRUG_NAME'] = np.where(drugset.DRUG_CODE == '7444',
                                    '4-HYDROXY-3-METHOXY-METHAMPHETAMINE',
                                    drugset.DRUG_NAME)
    drugset['DRUG_NAME'] = np.where(drugset.DRUG_CODE == '9740',
                                    'SUFENTANIL',
                                    drugset.DRUG_NAME)
    drugset = drugset[~drugset.DRUG_NAME.str.contains('DELETE THIS RECORD')]
    drugset = (drugset[['DRUG_CODE', 'DRUG_NAME']]
               .drop_duplicates()
               .reset_index(drop=True))
    assert drugset.DRUG_CODE.is_unique
    return drugset


def clean_states(report_final_dict, statelist, elim_list=None):
    assert all([set(val['STATE']).issubset(set(statelist))
                for key, val in report_final_dict.items()
                if key not in ['7']])
    r = {key: val.assign(STATE=np.where(val['STATE'].str.contains('GUAM'),
                                        'GUAM', val['STATE']))
         if key not in ['7'] else val
         for key, val in report_final_dict.items()}
    if elim_list:
        elstates = pd.DataFrame({'STATE': elim_list})
        r = {key:
             (val.merge(elstates, how='left', indicator=True)
                 .query('_merge=="left_only"')
                 .reset_index(drop=True)
                 .drop(columns='_merge'))
             if key not in ['7'] else val
             for key, val in r.items()}
    print('Number of states in each report:')
    print({key: len(set(df.STATE)) for key, df in r.items()
           if 'STATE' in df.columns})
    return r


def clean_zips(report_final_dict):
    report_final_dict['1'] = (get_rid_of_bad_zips(report_final_dict['1'],
                                                  zip3_state_crosswalk_file)
                              .rename(columns={'ZIP': 'ZIP3'}))
    return report_final_dict


def clean_business_activities(report_final_dict):
    if '5' in report_final_dict:
        report_final_dict['5'] = report_final_dict['5'].assign(
            BUSINESS_ACTIVITY=np.where(
                report_final_dict['5'].BUSINESS_ACTIVITY.str.endswith(
                    'NARCOTIC TREATMENT PROGRAMS'),
                'N-U NARCOTIC TREATMENT PROGRAMS',
                report_final_dict['5'].BUSINESS_ACTIVITY))
    if '7' in report_final_dict:
        report_final_dict['7'] = report_final_dict['7'].assign(
            BUSINESS_ACTIVITY=np.where(
                report_final_dict['7'].BUSINESS_ACTIVITY.str.endswith(
                    'NARCOTIC TREATMENT PROGRAMS'),
                'N-U NARCOTIC TREATMENT PROGRAMS',
                report_final_dict['7'].BUSINESS_ACTIVITY))
    return report_final_dict

# druglist = ['METHADONE', 'HYDROMORPHONE', 'BUPRENORPHINE', 'FENTANYL BASE', 'HYDROCODONE', 'OXYCODONE', 'MORPHINE', 'MEPERIDINE (PETHIDINE)', 'OXYMORPHONE', 'CODEINE']


def clean_time_vars(report_final_dict, report_sortvars):
    return {reportno: rpt.pipe(encode_time_variables,
                               report_sortvars[reportno])
            for reportno, rpt in report_final_dict.items()}


def encode_time_variables(rpt, sortvars):
    if 'Q' in rpt.columns:
        rpt['QUARTER'] = pd.to_datetime(
            rpt['YEAR'].astype(str) + 'Q' + rpt['Q'].astype(str))
        rpt = rpt.drop(columns='Q')
    if 'YEAR' in rpt.columns:
        rpt['YEAR'] = rpt.YEAR.astype(int)
    rpt = rpt.groupby(sortvars, as_index=False).sum()
    return rpt


def score_outliers(df, var, method='std_above_mean'):
    if method == 'LocalOutlierFactor':
        from sklearn.neighbors import LocalOutlierFactor
        x = df[var].tolist()
        nn = min([len(x)-1, 20])
        clf = LocalOutlierFactor(n_neighbors=nn, contamination=0.1)
        y_pred = clf.fit_predict(np.array(x).reshape(-1, 1))
        sum(y_pred == -1)
        X_scores = clf.negative_outlier_factor_
        df = pd.concat(
            [df.reset_index(drop=True),
             pd.DataFrame({'GRAMS2': x, 'OUTLIER_SCORE': X_scores})], axis=1)
        assert df[var].equals(df.GRAMS2)
        return df.drop(columns='GRAMS2')
    elif method == 'std_above_mean':
        _std = np.std(df[var])
        _mean = np.mean(df[var])
        return df.assign(OUTLIER_SCORE=lambda df: (df[var] - _mean)/_std)


def handle_outliers(rpt, reportno, grams_name_dict, sortvars, timevars,
                    replace=False, threshold=None):
    # rpt = pd.DataFrame()
    # rpt = report_final_dict[reportno].copy()
    if reportno == '4':
        return rpt
    rpt = rpt.sort_values(sortvars[reportno]).reset_index(drop=True)
    svar_notime = [x for x in sortvars[reportno] if x not in timevars]
    svar_time = [x for x in sortvars[reportno] if x in timevars]
    gname = grams_name_dict[reportno]

    grouped = rpt.groupby(svar_notime)

    l_grouped = list(grouped)
    rpt = pd.concat([score_outliers(x[1], gname) for x in l_grouped])
    dropvars = ['OUTLIER_SCORE'] + [x for x in rpt.columns if x == 'BUYERS']

    rpt = pd.concat([rpt, (rpt
                           .drop(columns=svar_time + dropvars)
                           .groupby(svar_notime)
                           .shift(1)
                           .rename(columns={gname: 'pl1'}))], axis=1)
    rpt = pd.concat([rpt, (rpt
                           .drop(columns=svar_time + ['pl1'] + dropvars)
                           .groupby(svar_notime)
                           .shift(-1)
                           .rename(columns={gname: 'min1'}))], axis=1)
    rpt['OUTLIER_REPLACE'] = (rpt.min1 + rpt.pl1)/2
    rpt.loc[rpt.OUTLIER_REPLACE.isnull()
            & rpt.min1.notnull(), 'OUTLIER_REPLACE'] = rpt.min1
    rpt.loc[rpt.OUTLIER_REPLACE.isnull()
            & rpt.pl1.notnull(), 'OUTLIER_REPLACE'] = rpt.pl1
    rpt = rpt.drop(columns=['min1', 'pl1'])
    if threshold:
        print('Number of entries above threshold: ',
              rpt.query('OUTLIER_SCORE > %s' % threshold).shape[0])
    return rpt


def generate_outlier_indicators(report_final_dict, timevars, threshold):
    report_final_dict = {reportno:
                         (rpt.pipe(handle_outliers, reportno, grams_name_dict,
                                   report_sortvars, timevars,
                                   threshold=threshold))
                         for reportno, rpt in report_final_dict.items()}
    return report_final_dict


    # ra = rpt.drop(columns=['YEAR', 'Q', 'average']).groupby(['STATE', 'ZIP', 'DRUG_CODE', 'DRUG_NAME']).transform(lambda x: x.rolling(4, 2).mean()).rename(columns={'GRAMS': 'ma'})
    # rpt = pd.concat([rpt, ra[1:].reset_index(drop=True)], axis=1)
    # rpt['ma'] = np.where(rpt.ma.isnull(), ra.ma, rpt.ma)
    # rpt['ratio'] = (rpt.GRAMS/rpt.ma)
# 
    # drug_zip_problems = rpt[rpt.DRUG_NAME.isin(druglist)].query('ma>20 and (ratio>3 or ratio2>10)') 
    # drug_zip_problems = drug_zip_problems.merge(pd.DataFrame(dict(DRUG_CODE=['9801', '9801', '9143'], ZIP=['297', '571', '837'], YEAR=['2013','2011','2017'], Q=['1','2','1'])).merge(rpt), how='outer')
    # drug_zip_problems['query'] = 'DRUG_CODE=="' + drug_zip_problems['DRUG_CODE'] + '" and ZIP=="' + drug_zip_problems['ZIP'] + '" and YEAR<='+(drug_zip_problems['YEAR'].astype(int) + 7).astype(str) + ' and YEAR>='+(drug_zip_problems['YEAR'].astype(int) - 7).astype(str)
    # for i, row in drug_zip_problems.T.iteritems():
    #     print(rpt.assign(YEAR=lambda df: df.YEAR.astype(int)).query(row['query']).merge(pd.DataFrame(row).T.drop(columns='query').assign(YEAR=lambda df: df.YEAR.astype(int)), how='outer', indicator=True)) 
# 
# 
    # [['DRUG_CODE', 'ZIP', 'YEAR']]
    # drug_zip_problem_pairs = drug_zip_problem_pairs.append(pd.DataFrame(dict(DRUG_CODE=['9801', '9801', '9143'], ZIP=['297', '571', '837'], YEAR=['2013','2011','2017'])))
    # queries = ['DRUG_CODE=="%s" and ZIP=="%s" and YEAR<=%s and YEAR>=%s' % (x[0], x[1], int(x[2])+7, int(x[2])-7) for x in drug_zip_problem_pairs.values.tolist()]


# CONFORMING CCHECKS rpt.BUSINESS_ACTIVITY etc
# assert set(rpt['STATE']).issubset(set(statelist))


#     for reportno in list_of_reports:
# 
#         rpt = pd.DataFrame()
#         rpt = report_final_dict[reportno].copy()
# 
#         if reportno == '1':
#             rpt['GRAMS'] = rpt.apply(lambda x: lambda_for_hand_fix(x, reportno), axis=1)
#             # rpt = get_rid_of_bad_zips(rpt, zip3_state_crosswalk_file)
#             # rpt['QUARTER'] = pd.to_datetime(rpt['YEAR'].astype(str) + 'Q' + rpt['Q'].astype(str))
#             # rpt = rpt.rename(columns={'ZIP': 'ZIP3'})
#             # rpt = rpt[[x for x in rpt.columns if x not in ['Q']]]
#             # rpt = rpt.groupby(['DRUG_CODE', 'DRUG_NAME', 'STATE', 'YEAR', 'ZIP3', 'QUARTER']).sum()
# 
#         if reportno == '2':
#             rpt['GRAMS'] = rpt.apply(lambda x: lambda_for_hand_fix(x, reportno), axis=1)
#             rpt['QUARTER'] = pd.to_datetime(rpt['YEAR'].astype(str) + 'Q' + rpt['Q'].astype(str))
#             rpt = rpt[[x for x in rpt.columns if x not in ['Q']]]
#             rpt = rpt.groupby(['DRUG_CODE', 'DRUG_NAME', 'STATE', 'YEAR', 'QUARTER']).sum()
# 
#         if reportno == '3':
#             rpt['GRAMS_PC'] = rpt.apply(lambda x: lambda_for_hand_fix(x, reportno), axis=1)
#             rpt['QUARTER'] = pd.to_datetime(rpt['YEAR'].astype(str) + 'Q' + rpt['Q'].astype(str))
#             rpt = rpt[[x for x in rpt.columns if x not in ['Q']]]
#             rpt = rpt.groupby(['DRUG_CODE', 'DRUG_NAME', 'STATE', 'YEAR', 'QUARTER']).sum()
# 
#         if reportno == '4':
#             rpt = rpt.groupby(['STATE', 'YEAR']).sum()
# 
#         if reportno == '5':
#             rpt['TOTAL GRAMS'] = rpt.apply(lambda x: lambda_for_hand_fix(x, reportno), axis=1)
#             rpt = rpt.groupby(['DRUG_CODE', 'DRUG_NAME', 'STATE', 'YEAR', 'BUSINESS ACTIVITY']).sum()
# 
#         if reportno == '7':
#             rpt['TOTAL GRAMS'] = rpt.apply(lambda x: lambda_for_hand_fix(x, reportno), axis=1)
#             rpt = rpt.groupby(['DRUG_CODE', 'DRUG_NAME', 'YEAR', 'BUSINESS ACTIVITY']).sum()
# 
#         rpt = rpt.rename(columns={'BUSINESS ACTIVITY': 'BUSINESS_ACTIVITY',
#                                   'TOTAL GRAMS': 'TOTAL_GRAMS'})
#         if reportno != '4':
#             assert rpt.query('DRUG_CODE == "9003"').shape[0] < 5
#             rpt = rpt.query('DRUG_CODE != "9003"')
# 
#         rpt.to_stata(os.path.join(folder, 'Report%s.dta' % reportno))
#         report_final_dict[reportno] = rpt
# 
#     return report_final_dict, drugset


def process_cols(df,
                 excl_cols=['PAGE', 'TOTAL', 'AVG GRAMS'],
                 ren_cols={'DRUG CODE': 'DRUG_CODE',
                           'DRUG NAME': 'DRUG_NAME',
                           'BUSINESS ACTIVITY': 'BUSINESS_ACTIVITY',
                           'TOTAL GRAMS': 'TOTAL_GRAMS'}):
    """
    drop any unused columns and do some renames
    """
    df = df.copy()
    use_cols = sorted([x for x in df.columns if x not in excl_cols])
    df = df[use_cols].rename(columns=ren_cols)
    return df


def remove_total_lines(df, reportno):
    """
    get rid of total lines, which summarize totals at the state or US level
    """
    if reportno in ['1', '5']:
        assert set(df['STATE']).issubset(set(statelist))
    if reportno == '1':
        remove_list = [x for x in set(df['GEO'])
                       if x.isdigit() is False]
    if reportno == '2' or reportno == '3':
        remove_list = [x for x in set(df['GEO'])
                       if x not in statelist]
    if reportno == '1' or reportno == '2' or reportno == '3':
        df = (df
              .merge(pd.DataFrame(dict(GEO=remove_list)),
                     how='left', indicator=True)
              .query('_merge=="left_only"')
              .drop(columns='_merge'))
    if reportno == '1':
        assert [x for x in set(df['GEO']) if x.isdigit() is False] == []
        df['GEO'] = df['GEO'].apply(lambda x: '%03d' % int(x))
        assert df[df.apply(lambda x: len(x['GEO']) != 3, axis=1)].shape[0] == 0
    if reportno == '2' or reportno == '3':
        assert [x for x in set(df['GEO']) if x not in statelist] == []
    return df


def fix_quarter_quantity_columns(df, reportno):
    """
    Converts quarter columns to floats, removing commas,
    and fixes weird change in 2011 that multiplied everything in
    R3 by 1000
    """
    quarter_columns = [x for x in df.columns if len(x) == 2
                       and x.startswith('Q') and int(x[1:]) in range(1, 5)]

    if reportno == '1' or reportno == '2' or reportno == '3':

        df = df.assign(**{x: df[x].str.replace(',', '').astype(float)
                          for x in quarter_columns})

    if reportno == '3':
        for col in quarter_columns:
            # rpt[col][rpt['YEAR'] == '2011'] = rpt[col] / 1000
            df[col] = df.apply(
                lambda df: (df[col]/1000 if df['YEAR'] == '2011'
                            else df[col]), axis=1)
    return df


def reshape_long(df, reportno, grams_name_dict, geo_name_dict):
    """
    For dfs with four quarter, reshape to long
    """
    if reportno == '1' or reportno == '2' or reportno == '3':

        usecols = [x for x in df.columns if not x.startswith('Q')]
        grams_title = grams_name_dict[reportno]

        df = (df
              .set_index(usecols)
              .stack()
              .reset_index()
              .rename(columns={0: grams_title}))
        rename_col = {y: 'Q' for y
                      in [x for x in df.columns
                          if x not in usecols + [grams_title]]}
        df = df.rename(columns=rename_col)
        df = df.assign(Q=df.Q.str.split('Q').str[1])
        df = df.rename(columns={'GEO': geo_name_dict[reportno]})
    return df


def report_4_to_populations(df, reportno):
    if reportno == '4':
        df = df.assign(POP=(df['2010 POP'] if '2010 POP' in df.columns
                            else df['2000 POP']))
        df = df.assign(POP=(np.where(df['POP'].isnull(),
                                     df['2000 POP'],
                                     df['POP'])
                            if '2000 POP' in df.columns
                            else df['POP']))
        df = df[['STATE', 'YEAR', 'POP']].assign(
            POP=df['POP'].str.replace(',', '').astype(float)).drop_duplicates()
        remove_list = [x for x in set(df['STATE']) if x not in statelist]
        df = (df
              .merge(pd.DataFrame(dict(STATE=remove_list)),
                     how='left', indicator=True)
              .query('_merge=="left_only"')
              .drop(columns='_merge'))
        df = df.sort_values(['STATE', 'YEAR']).reset_index(drop=True)
    return df


def business_activity_fixes(df, reportno):
    if reportno == '5' or reportno == '7':
        df['BUSINESS_ACTIVITY'] = df['BUSINESS_ACTIVITY'].apply(
            lambda x: x.split(' - ')[-1:][0])
        df['BUYERS'] = df['BUYERS'].apply(
            lambda x: int(str(x).replace(',', '')))
        df['TOTAL_GRAMS'] = df['TOTAL_GRAMS'].apply(
            lambda x: float(str(x).replace(',', '')))
    return df


def get_rid_of_bad_zips(rpt, zip3_state_crosswalk_file):
    '''
    note: also gets rid of zip3 in the territories
    '''
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


# def lambda_guam(x):
#     if 'GUAM' in x:
#         return 'GUAM'
#     else:
#         return x


def lambda_to_fix_drugname(df):
    if 'DRUGNAME:' in df['DRUG_CODE']:
        return df['DRUG_CODE'].split('DRUGNAME:')[1].strip()
    return df['DRUG_NAME']


def lambda_to_fix_drugcode(df):
    if 'DRUGNAME:' in df['DRUG_CODE']:
        return df['DRUG_CODE'].split('DRUGNAME:')[0].strip()
    return df['DRUG_CODE']
    # if drugcode == '9041L':
    #     drugcode = '9041'


def split_off_drug_code_from_drug_name(df):
    if 'DRUG_CODE' in df.columns:
        df['DRUG_NAME'] = df[['DRUG_NAME', 'DRUG_CODE']].apply(
            lambda x: lambda_to_fix_drugname(x), axis=1)
        df['DRUG_CODE'] = df[['DRUG_NAME', 'DRUG_CODE']].apply(
            lambda x: lambda_to_fix_drugcode(x), axis=1)
    return df


#def lambda_to_replace_drugname(df, drugset_dict):
#    return drugset_dict[df['DRUG_CODE']]
#
#
#def lambda_for_hand_fix(df, reportno):
#
#    if reportno in ['1', '2']:
#        grams = df['GRAMS']
#
#    if reportno in ['3']:
#        grams = df['GRAMS_PC']
#
#    if reportno in ['5', '7']:
#        grams = df['TOTAL GRAMS']
#
#    if reportno == '1' and df['ZIP'] == '297' and df['STATE'] == "SOUTH CAROLINA" and df['YEAR'] == '2013' and df['Q'] == 1 and df['DRUG_NAME'] == 'FENTANYL BASE' and df['GRAMS'] > 78000:
#        grams = 89.48750305175781
#        print('report %s, altering fentanyl grams' % reportno)
#
#    if reportno == '1' and df['ZIP'] == '571' and df['STATE'] == "SOUTH DAKOTA" and df['YEAR'] == '2011' and df['Q'] == 2 and df['DRUG_NAME'] == 'FENTANYL BASE' and df['GRAMS'] > 1400:
#        grams = 217.6462554931641
#        print('report %s, altering fentanyl grams' % reportno)
#
#    if reportno == '1' and df['ZIP'] == '837' and df['STATE'] == "IDAHO" and df['YEAR'] == '2017' and df['Q'] == 1 and df['DRUG_NAME'] == 'OXYCODONE' and df['GRAMS'] > 100000:
#        grams = 13671.485
#        print('report %s, altering Idaho oxycodone grams for 837' % reportno)
#
#    if reportno == '2' and df['STATE'] == "SOUTH CAROLINA" and df['YEAR'] == '2013' and df['Q'] == 1 and df['DRUG_NAME'] == 'FENTANYL BASE' and df['GRAMS'] > 78000:
#        grams = 1632.327522277832
#        print('report %s, altering fentanyl grams' % reportno)
#
#    if reportno == '2' and df['STATE'] == "SOUTH DAKOTA" and df['YEAR'] == '2011' and df['Q'] == 2 and df['DRUG_NAME'] == 'FENTANYL BASE' and df['GRAMS'] > 1400:
#        grams = 443.3562550544739
#        print('report %s, altering fentanyl grams' % reportno)
#
#    if reportno == '3' and df['STATE'] == "SOUTH CAROLINA" and df['YEAR'] == '2013' and df['Q'] == 1 and df['DRUG_NAME'] == 'FENTANYL BASE':
#        grams = 100000 * 1632.327522277832 / 4625364
#        print('report %s, altering fentanyl grams' % reportno)
#
#    if reportno == '3' and df['STATE'] == "SOUTH DAKOTA" and df['YEAR'] == '2011' and df['Q'] == 2 and df['DRUG_NAME'] == 'FENTANYL BASE':
#        grams = 100000 * 443.3562550544739 / 814180
#        print('report %s, altering fentanyl grams' % reportno)
#
#    if reportno == '5' and df['STATE'] == "SOUTH CAROLINA" and df['YEAR'] == '2013' and df['DRUG_NAME'] == 'FENTANYL BASE' and df['BUSINESS ACTIVITY'] == 'HOSPITALS':
#        grams = df['TOTAL GRAMS'] - (78209.6484375 - 89.48750305175781)
#        print('report %s, altering fentanyl grams' % reportno)
#
#    if reportno == '5' and df['STATE'] == "SOUTH DAKOTA" and df['YEAR'] == '2011' and df['DRUG_NAME'] == 'FENTANYL BASE' and df['BUSINESS ACTIVITY'] == 'HOSPITALS':
#        grams = df['TOTAL GRAMS'] - (14240.349609375 - 217.6462554931641)
#        print('report %s, altering fentanyl grams' % reportno)
#
#    if reportno == '7' and df['YEAR'] == '2013' and df['DRUG_NAME'] == 'FENTANYL BASE' and df['BUSINESS ACTIVITY'] == 'HOSPITALS':
#        grams = df['TOTAL GRAMS'] - (78209.6484375 - 89.48750305175781)
#        print('report %s, altering fentanyl grams' % reportno)
#
#    if reportno == '7' and df['YEAR'] == '2011' and df['DRUG_NAME'] == 'FENTANYL BASE' and df['BUSINESS ACTIVITY'] == 'HOSPITALS':
#        grams = df['TOTAL GRAMS'] - (14240.349609375 - 217.6462554931641)
#        print('report %s, altering fentanyl grams' % reportno)
#
#    return grams
