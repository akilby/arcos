def internal_consistency_check(Reports_dict, reportnos=None):
    return_dict = {}
    if reportnos:
        search_list = reportnos
    else:
        search_list = list(Reports_dict.keys())
    for reportno in search_list:
        rdf = pd.DataFrame()
        rdf = Reports_dict[reportno].copy()
        print('REPORT', reportno)
        if not rdf.empty:
            if reportno == '1':
                add_down_dont_match = check_add_down(rdf=rdf, tot_col='GEO', columns_to_add=['Q1', 'Q2', 'Q3', 'Q4', 'TOTAL'], groupby_vars=['YEAR', 'DRUG_CODE', 'STATE'])
                add_across_dont_match = check_add_across(rdf=rdf, columns_to_add=['Q1', 'Q2', 'Q3', 'Q4'], col_tot=['TOTAL'])
                return_dict[reportno] = (add_down_dont_match, add_across_dont_match)
                assert return_dict[reportno] == ({}, {})
            elif reportno == '2':
                add_down_dont_match = check_add_down(rdf=rdf, tot_col='GEO', columns_to_add=['Q1', 'Q2', 'Q3', 'Q4', 'TOTAL'], groupby_vars=['YEAR', 'DRUG_CODE'])
                add_across_dont_match = check_add_across(rdf=rdf, columns_to_add=['Q1', 'Q2', 'Q3', 'Q4'], col_tot=['TOTAL'])
                return_dict[reportno] = (add_down_dont_match, add_across_dont_match)
                assert return_dict[reportno] == ({}, {})
            elif reportno == '3':
                add_across_dont_match = check_add_across(rdf=rdf, columns_to_add=['Q1', 'Q2', 'Q3', 'Q4'], col_tot=['TOTAL'])
                return_dict[reportno] = (add_across_dont_match)
                assert return_dict[reportno] == {}
            elif reportno == '4':
                # 4 is the only report with unexplainable internal inconsistencies, for American Samoa in 2002
                add_down_dont_match = check_add_down(rdf=rdf, tot_col='STATE', columns_to_add=['TOTAL GRAMS'], groupby_vars=['YEAR', 'DRUG_CODE'])

                rdf['POP'] = np.nan
                if '2000 POP' in list(rdf.columns):
                    rdf['POP'][rdf['2000 POP'].notnull()] = rdf['2000 POP']
                if '2010 POP' in list(rdf.columns):
                    rdf['POP'][rdf['2010 POP'].notnull()] = rdf['2010 POP']

                divisor_dont_match = check_divide(rdf, 'TOTAL GRAMS', 'POP', 'GRAMS/100K POP', 100000)
                assert all(divisor_dont_match[('TOTAL GRAMS', 'POP', 'GRAMS/100K POP')]['STATE'] == 'AMERICAN SAMOA')
                return_dict[reportno] = (add_down_dont_match, divisor_dont_match)
            elif reportno == '5' or reportno == '7':
                divisor_dont_match = check_divide(rdf, 'TOTAL GRAMS', 'BUYERS', 'AVG GRAMS')
                return_dict[reportno] = (divisor_dont_match)
                assert return_dict[reportno] == {}
    return return_dict


def across_consistency_check(Reports_dict, reportlist):
    returndict = {}
    if reportlist == ['5', '7']:
        # There are some errors, almost entirely in 2011 but a few in buyers in 2014
        rdf5, rdf7 = pd.DataFrame(), pd.DataFrame()
        rdf5, rdf7 = Reports_dict['5'].copy(), Reports_dict['7'].copy()
        assert check_divide(rdf5, 'TOTAL GRAMS', 'BUYERS', 'AVG GRAMS') == {}
        assert check_divide(rdf7, 'TOTAL GRAMS', 'BUYERS', 'AVG GRAMS') == {}
        returndict[('5', '7')] = groupby_across_sheets(big_df=rdf5[list(set(rdf5.columns) - {'AVG GRAMS'})], small_df=rdf7[list(set(rdf7.columns) - {'AVG GRAMS'})], groupby_vars=['YEAR', 'DRUG CODE', 'BUSINESS ACTIVITY'], compare_cols=['TOTAL GRAMS', 'BUYERS'])
        returndict2, returnlist_unmatch = returndict[('5', '7')]
        assert all(returnlist_unmatch['YEAR'] == '2011')
        for key in returndict2:
            assert sum(returndict2[key]['YEAR'] == '2011') + sum(returndict2[key]['YEAR'] == '2014') == len(returndict2[key])

    if reportlist == ['2', '3', '4']:
        # All errors are in US totals only. But there
        rdf2, rdf3, rdf4 = pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
        rdf2, rdf3, rdf4 = Reports_dict['2'].copy(), Reports_dict['3'].copy(), Reports_dict['4'].copy()

        target_us_row_title = list(set(rdf3['GEO']) - set(statelist))[0]
        for item in set(rdf2['GEO']) - set(statelist):
            rdf2['GEO'].loc[rdf2['GEO'] == item] = target_us_row_title

        for item in set(rdf3['GEO']) - set(statelist):
            rdf3['GEO'].loc[rdf3['GEO'] == item] = target_us_row_title

        for item in set(rdf4['STATE']) - set(statelist):
            rdf4['STATE'].loc[rdf4['STATE'] == item] = target_us_row_title

        assert all(rdf3.columns == rdf2.columns)

        for col in ['Q1', 'Q2', 'Q3', 'Q4', 'TOTAL']:
            rdf3[col] = rdf3[col].apply(lambda x: float(str(x).replace(',', '')))
            rdf3[col][rdf3['YEAR'] == '2011'] = rdf3[col] / 1000

        rdf4['POP'] = np.nan
        if '2000 POP' in list(rdf4.columns):
            rdf4['POP'][rdf4['2000 POP'].notnull()] = rdf4['2000 POP']
        if '2010 POP' in list(rdf4.columns):
            rdf4['POP'][rdf4['2010 POP'].notnull()] = rdf4['2010 POP']

        df_pop = pd.DataFrame([(float(str(x[0]).replace(',', '')), x[1], x[2]) for x in list(set([tuple(x) for x in rdf4[['POP', 'YEAR', 'STATE']].values]))])
        df_pop.columns = ['POP', 'YEAR', 'GEO']
        rdf2 = pd.merge(rdf2, df_pop, on=['GEO', 'YEAR'])

        merged = pd.merge(rdf2, rdf3, how='outer', on=['DRUG_CODE', 'GEO', 'YEAR'], indicator=True)
        missing_entries = merged[merged['_merge'] == 'right_only']
        returndict[('5', '7', 'missing_entries')] = missing_entries
        merged2 = pd.merge(rdf2, rdf3, how='inner', on=['DRUG_CODE', 'GEO', 'YEAR'], )

        for s1 in range(1, 5):
            d = check_divide(merged2, 'Q%s_x' % s1, 'POP', 'Q%s_y' % s1, 100000)
            assert set(list(d.values())[0]['GEO']) == {target_us_row_title}
            returndict[('5', '7', 'Q%s' % s1)] = d

    if reportlist == ['1', '2']:
        rdf1, rdf2 = pd.DataFrame(), pd.DataFrame()
        rdf1, rdf2 = Reports_dict['1'].copy(), Reports_dict['2'].copy()
        for tot in [x for x in set(rdf1['GEO']) if x.isdigit() is False]:
            rdf1 = rdf1.loc[rdf1['GEO'] != tot]
        for us in [x for x in set(rdf2['GEO']) if x not in statelist]:
            rdf2 = rdf2.loc[rdf2['GEO'] != us]
        rdf2['STATE'] = rdf2['GEO']
        returndict[('1', '2')] = groupby_across_sheets(big_df=rdf1, small_df=rdf2, groupby_vars=['YEAR', 'DRUG_CODE', 'STATE'], compare_cols=['Q1', 'Q2', 'Q3', 'Q4', 'TOTAL'])
    if reportlist == ['2', '5']:
        rdf2, rdf5 = pd.DataFrame(), pd.DataFrame()
        rdf2, rdf5 = Reports_dict['2'].copy(), Reports_dict['5'].copy()
        rdf2 = rdf2.loc[rdf2['GEO'] != 'UNITED STATES']
        rdf2['STATE'] = rdf2['GEO']
        rdf2['TOTAL GRAMS'] = rdf2['TOTAL']
        rdf2['DRUG CODE'] = rdf2['DRUG_CODE']
        returndict[('2', '5')] = groupby_across_sheets(big_df=rdf5, small_df=rdf2, groupby_vars=['YEAR', 'DRUG CODE', 'STATE'], compare_cols=['TOTAL GRAMS'])
    return returndict


def groupby_across_sheets(bigdf, smalldf, groupby_vars, compare_cols):
    big_df = bigdf.copy()
    small_df = smalldf.copy()
    returndict2 = {}
    for col in compare_cols:
        big_df[col] = big_df[col].apply(lambda x: float(str(x).replace(',', '')))
        small_df[col] = small_df[col].apply(lambda x: float(str(x).replace(',', '')))
    big_df_test = big_df.groupby(groupby_vars).sum()
    merged_rdf = pd.merge(big_df_test, small_df, right_on=groupby_vars, left_index=True, how='outer', indicator=True)
    returnlist_unmatch = merged_rdf[merged_rdf['_merge'] != 'both']
    merged_rdf = pd.merge(big_df_test, small_df, right_on=groupby_vars, left_index=True, how='inner', indicator=True)
    for col in compare_cols:
        colx = col + '_x'
        coly = col + '_y'
        df_nonmatch = merged_rdf[merged_rdf.apply(lambda x: are_close(x[colx], x[coly], 0.015) is False, axis=1)]
        if len(df_nonmatch) > 0:
            returndict2[col] = df_nonmatch
    return returndict2, returnlist_unmatch


def check_add_down(rdf, tot_col, columns_to_add, groupby_vars):
    rdfa = pd.DataFrame()
    rdfa = rdf.copy()
    tot_loc = {tot_col: [x for x in list(set(rdfa[tot_col].tolist())) if x in totallist]}
    add_down_dont_match = {}
    tot_strings = list(tot_loc.values())[0]
    for col in columns_to_add:
        rdfa[col] = rdfa[col].apply(lambda x: float(x.replace(',', '')))
    rdfa['bin'] = rdfa[list(tot_loc.keys())[0]].apply(lambda x: x in tot_strings)
    rdf_test = rdfa.groupby(groupby_vars + ['bin']).sum()
    pctc = pd.DataFrame(round(abs(rdf_test.groupby(groupby_vars).pct_change())))
    totdiv = 0
    for year in set(rdfa['YEAR']):
        div = len(set(rdfa['bin'][rdfa['YEAR'] == year]))
        for v in groupby_vars:
            div = div * len(set(rdfa[v][rdfa['YEAR'] == year]))
        totdiv = totdiv + div
        entries = len(pctc) / totdiv
    assert 0.6 <= entries <= 1
    for column_to_add in columns_to_add:
        r = rdf_test.loc[pctc[column_to_add].notnull() & pctc[column_to_add] != 0]
        if len(r) > 0:
            add_down_dont_match[column_to_add] = r
    return add_down_dont_match


def check_add_across(rdf, columns_to_add, col_tot):
    rdfa = pd.DataFrame()
    rdfa = rdf.copy()
    add_across_dont_match = {}
    for col in columns_to_add + col_tot:
        rdfa[col] = rdfa[col].apply(lambda x: float(x.replace(',', '')))
    r = rdfa.loc[round(rdfa[columns_to_add].sum(axis=1) - rdfa[col_tot[0]], 1) != 0]
    if len(r) > 0:
        add_across_dont_match[[tuple(columns_to_add + col_tot)]] = r
    return add_across_dont_match


def check_divide(rdf, top_divisor, bot_divisor, equals_to, multiplier=1, tolerance=0.02):
    rdfa = pd.DataFrame()
    rdfa = rdf.copy()
    divide_dont_match = {}
    for col in [top_divisor, bot_divisor, equals_to]:
        rdfa[col] = rdfa[col].apply(lambda x: float(str(x).replace(',', '')))
    rdfa['CALCULATED'] = rdfa.apply(lambda x: custom_lambda(x, top_divisor, bot_divisor, equals_to, multiplier, 'calc'), axis=1)
    rdfa['BOOL'] = rdfa.apply(lambda x: custom_lambda(x, top_divisor, bot_divisor, equals_to, multiplier, 'bool'), axis=1)
    rdfa['CLOSE'] = rdfa.apply(lambda x: custom_lambda(x, top_divisor, bot_divisor, equals_to, multiplier, tolerance), axis=1)
    if len(rdfa.loc[-rdfa['CLOSE']]) > 0:
        columns_list = [x for x in list(rdfa.columns) if ('_x' not in x and '_y' not in x) or x in [top_divisor, bot_divisor, equals_to]]
        divide_dont_match[(top_divisor, bot_divisor, equals_to)] = rdfa[columns_list].loc[-rdfa['CLOSE']]
    return divide_dont_match


def custom_lambda(df, top_divisor, bot_divisor, equals_to, multiplier, returntype):
    calculated = multiplier * df[top_divisor] / df[bot_divisor]
    res = are_equal(calculated, df[equals_to])
    if returntype == 'bool':
        return res[0]
    if type(returntype) is float:
        return are_close(calculated, df[equals_to], returntype)
    return res[1]


def are_equal(val_compare, reference_val):
    r = return_round(reference_val)
    val = round(val_compare, r)
    comp = round(reference_val, r)
    val1 = round(val_compare, r + 1)
    comp1 = round(reference_val, r + 1)
    return (val == comp or val1 == comp1), (val, comp, val1, comp1)


def are_close(val_compare, reference_val, tolerance):
    b, (val, comp, val1, comp1) = are_equal(val_compare, reference_val)
    if not b:
        if reference_val != 0:
            return (abs(val_compare - reference_val) / reference_val <= tolerance) or (abs(val - comp) <= tolerance)
        else:
            return (abs(val - comp) <= tolerance)
    return b


def return_round(x):
    if x > 0:
        if int(math.log10(x)) == math.log10(x) and int(math.log10(x)) < 0:
            magn = int(math.log10(x)) + 1
        else:
            magn = int(math.log10(x))
        if magn < 0:
            return -(magn - 1)
        elif magn == 0 or magn == 1 or magn == 2:
            return 2
        elif magn == 3 or magn == 4:
            return 1
        else:
            return 0
    elif x == 0:
        return 2
    else:
        raise Exception("shouldn't be less than zero")

        