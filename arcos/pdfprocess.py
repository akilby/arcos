import pdfminer
import itertools
import sys
import pandas as pd
import numpy as np


from .data.data import (list_of_titles, quarterlist, businessactivities,
                        geounitlist, column_titles, headervar_mapping, 
                        statetotallist)


def process_row(rowkey, row, header_dict, header_line,
                keys_sorted, rowdict, skip_next_row, df_main):
    t, val = categorize_lines(row, header_dict['REPORT'], header_line)
    val, skip_next_row = post_categorize_hand_fix(val, skip_next_row)
    header_dict = update_header_dict(t, val, header_dict)
    if t == header_line:
        df_main = add_line_df(t, val, header_dict, df_main)
    if t == 'header line':
        header_line = val
    if t == 'uncategorized':
        print(t, val)
        print('skip_next_row0', skip_next_row)
        print('\n')
        print('row:', t, val, rowdict)
        key_next = keys_sorted[keys_sorted.index(rowkey) + 1]
        rowlist2 = []
        for colkey2 in sorted([x for x in rowdict[key_next].keys()], key=lambda x: x[0]):
            rowlist2.append(rowdict[key_next][colkey2])
        t2, val2 = categorize_lines(rowlist2, header_dict['REPORT'], header_line)
        print('next row:', t2, val2)
        if t2 == 'uncategorized' or t2 == 'blank line':
            vala, valb = reconcat_list(val, header_dict['REPORT'], header_line), reconcat_list(val2, header_dict['REPORT'], header_line)
            print('vala, valb', vala, valb)
            combine_row, skip_next_row = return_valid_combined_row(t, val, vala, valb, header_dict, header_line, skip_next_row)
            t3, val3 = categorize_lines(combine_row, header_dict['REPORT'], header_line)
            if t3 == header_line:
                df_main = add_line_df(t3, val3, header_dict, df_main)
                if skip_next_row == 0:
                    skip_next_row = 1
            elif set(t3).issubset(list(header_dict.keys())):
                for i in range(len(t3)):
                    header_dict[t3[i]] = [val3[i]]
                    if skip_next_row == 0:
                        skip_next_row = 1
            elif t3 == 'header line':
                header_line = val3
                if skip_next_row == 0:
                    skip_next_row = 1
            else:
                print('\n', t, val)
                print('\n', t2, val2)
                raise Exception('Above two rows uncategorized!')
        else:
            print('\n', t, val)
            print(rowdict)
            raise Exception('This row uncategorized (1)!:', t, val)
    return header_dict, header_line, skip_next_row, df_main


def categorize_lines(line, report, header_line):
    line = reconcat_list(line, report, header_line)
    str_search = ' '.join(line)
    if not report:
        if find_headvars(str_search):
            t, val = find_headvars(str_search)
        elif str_search == '' or line == [''] or line == []:
            t, val = 'blank line', line
        else:
            t, val = 'uncategorized', line
    elif set(report).issubset(['1', '2', '3']):
        test1 = [[x for x in line if x not in q] for q in quarterlist if len([x for x in line if x in q]) == len(q)]
        test1 = [x for x in test1 if len(x) == min([len(y) for y in test1])]
        test1 = [x[0] for x in test1 if len(test1) == 1 and len(test1[0]) == 1]
        if set(test1).issubset(set(geounitlist)) and test1 != [] and any([len([x for x in line if x in q]) == len(q) for q in quarterlist]):
            t, val = ['GEO_UNIT'], test1
        elif len(line) == 6 and all([is_num_not_nan(x) for x in line[1:]]) and (line[0:1][0] in statetotallist or line[0:1][0].isdigit()):
            t, val = header_line, line
        elif len(line) == 7 and sum([is_num_not_nan(x) for x in line[1:]]) == 5 and len([x for x in line[1:] if x == '']) == 1 and line[0:1][0] in statetotallist:
            t, val = header_line, [x for x in line if x != '']
        elif str_search == '' or line == [''] or line == ['SUBSTANCE'] or line == ['2']:
            t, val = 'blank line', line
        elif (len(line) < 6) and len(list(itertools.chain.from_iterable([x.split(' ') for x in line]))) == 6 and all([is_num_not_nan(x) for x in list(itertools.chain.from_iterable([x.split(' ') for x in line]))[1:]]):
            line_new = list(itertools.chain.from_iterable([x.split(' ') for x in line]))
            t, val = header_line, line_new
            print('last_cat')
        elif find_headvars(str_search):
            t, val = find_headvars(str_search)
        else:
            # print('str_search', str_search)
            t, val = 'uncategorized', line
    elif set(report).issubset(['4', '5', '7']):
        if report == ['7'] and str_search == 'REPORTING PERIOD: January 1, 2017 to December 31,2017':
            str_search = 'DATE RANGE: 01/01/2017 TO 12/31/2017'
        test1 = (sum([line == q for q in quarterlist]) == 1)
        if test1:
            t, val = 'header line', line
        elif str_search == 'RANK STATE POPULATION GRAMS TO DATE GRAMS/100K POP. TO DATE':
            t, val = 'header line', ['RANK', 'STATE', '2000 POP', 'TOTAL GRAMS', 'GRAMS/100K POP']
        elif report == ['4'] and len(line) == len(header_line) and line[0].isdigit() and line[1] in statetotallist and all([is_num_not_nan(x) for x in line[2:]]):
            t, val = header_line, line
        elif report == ['4'] and len(line) == 4 and line[0] in ['U.S. GRAMS / PER 100K:', 'U.S. GRAMS / PER 100K POPULATION', 'U.S. GRAMS/100K POP:'] and all([is_num_not_nan(x) for x in line[2:]]):
            t, val = header_line, ['N/A'] + line
        elif set(report).issubset(['5', '7']) and len(line) == len(header_line) and line[2].replace(',', '').isdigit() and all([is_num_not_nan(x) for x in line[3:]]):
            t, val = header_line, line
        elif set(report).issubset(['5', '7']) and len(line) == (len(header_line) + 1) and line[2].replace(',', '').isdigit() and all([any([is_num_not_nan(x), x == '']) for x in line[3:]]) and '' in line:
            t, val = header_line, [x for x in line if x != '']
        elif find_headvars(str_search):
            t, val = find_headvars(str_search)
        elif str_search == '' or line == [''] or line == ['SUBSTANCE'] or line == ['PROGRAMS']:
            t, val = 'blank line', line
        else:
            # print('str_search', str_search)
            t, val = 'uncategorized', line
    return t, val


def layout_to_coordinates_dict(layout):
    sys.stdout.flush()
    rowdict = {}
    for lt in layout:
        if isinstance(lt, pdfminer.layout.LTTextBoxHorizontal):
            for lt_small in lt:
                assert isinstance(lt_small,
                                  pdfminer.layout.LTTextLineHorizontal)
                (coord0, coord1, coord2, coord3) = (round(lt_small.bbox[0]),
                                                    round(lt_small.bbox[1]),
                                                    round(lt_small.bbox[2]),
                                                    round(lt_small.bbox[3]))
                lt_small_text = lt_small.get_text().replace('\n', '').strip()
                coord0, coord1, coord2, coord3, lt_small_text = ltcd_hand_fix(
                    coord0, coord1, coord2, coord3, lt_small_text)
                if (coord1, coord3) not in rowdict.keys():
                    rowdict[(coord1, coord3)] = {}
                rowdict[(coord1, coord3)][(coord0, coord2)] = lt_small_text
    return rowdict


def ltcd_hand_fix(coord0, coord1, coord2, coord3, lt_small_text):
    if ((coord0, coord1, coord2, coord3) == (257, 576, 535, 590) and
       lt_small_text == 'DISTRIBUTION BY STATE IN GRAMS PER 100,000 POPULATION'):
        print('hand fixing errant title coordinate:')
        print(coord0, coord1, coord2, coord3, lt_small_text)
        coord1 = 575
    return coord0, coord1, coord2, coord3, lt_small_text


def post_categorize_hand_fix(val, skip_next_row):
    # A HAND FIX
    if val == ['7540', 'METHYLONE (3,4-METHYLENEDIOXY-N-']:
        val = ['7540', 'METHYLONE (3,4-METHYLENEDIOXY-N-METHYLCATHINONE)']
        skip_next_row = 1
        print('need to skip next row')
    if (val == ['2100', 'BARBITURIC ACID DERIVIATIVE OR SALT [PER  21CFR'] or
       val == ['2100', 'BARBITURIC ACID DERIVIATIVE OR SALT [PER 21CFR']):
        val = ['2100', 'BARBITURIC ACID DERIVIATIVE OR SALT [PER  21CFR 1308.13(C)(3)]']
        skip_next_row = 1
        print('need to skip next row')
    if val == ['7365', 'DRONABINOL IN AN ORAL SOLUTION IN FDA APPROVED']:
        val = ['7365', 'DRONABINOL IN AN ORAL SOLUTION IN FDA APPROVED DRUG PRODUCT (SYNDROS - CII)']
        skip_next_row = 1
        print('need to skip next row')
    return val, skip_next_row


def initialize_header_dict():
    header_dict = {}
    for col in column_titles:
        header_dict[col] = ''
    return header_dict


def make_row(rowkey, rowdict):
    row = []
    for colkey in sorted([x for x in rowdict[rowkey].keys()], key=lambda x: x[0]):
        row.append(rowdict[rowkey][colkey])
    return row


def update_header_dict(t, val, header_dict):
    hd = header_dict.copy()
    if set(t).issubset(list(hd.keys())):
        for i in range(len(t)):
            hd[t[i]] = [val[i]]
    return hd


def add_line_df(t, val, header_dict, df_main):
    data_dict = {}
    for i in range(len(t)):
        data_dict[t[i]] = [val[i]]
    df = pd.DataFrame({**header_dict, **data_dict})
    df_main = pd.concat([df_main, df], axis=0, ignore_index=True)
    return df_main


def is_num_not_nan(s):
    if type(s) is float:
        if np.isnan(s):
            return False
    else:
        if s == 'nan':
            return False
        else:
            s = str(s).replace(',', '')
            try:
                complex(s)
            except ValueError:
                return False
    return True


def reconcat_list(val, report, header_line):
    separated = False
    for string_top in ['DRUG ENFORCEMENT ADMINISTRATION, OFFICE OF DIVERSION CONTROL', 'DRUG ENFORCEMENT ADMINISTRATION', 'DEPARTMENT OF JUSTICE']:
        val = [x.replace(string_top, '').strip() for x in val if x != string_top]
    if len(val) == 1:
        if ('--' in val[0] and len([x for x in val[0] if x == '-']) == len(val[0])) or val[0] == '-' or val[0] == '\x1a':
            print('DELETING, %s' % val[0])
            val = ['']
    if all([type(x) is str for x in val]):
        if set(report).issubset(['5', '7']) and len(val) != 1:
            separated = True
            first_stub = val[:1]
            val = val[1:]
        replacelist = [(x, x.replace(' ', '__')) for x in statetotallist + geounitlist + list(itertools.chain.from_iterable(quarterlist))]
        replacelist = replacelist + [('QUARTER__2ND__QUARTER', 'QUARTER 2ND__QUARTER'), ('QUARTER__3RD__QUARTER', 'QUARTER 3RD__QUARTER'), ('QUARTER__4TH__QUARTER', 'QUARTER 4TH__QUARTER')]
        val_l = [val[i] for i in range(len(val))]
        for i in range(len(val)):
            v = val[i]
            for q in range(len(replacelist)):
                    if replacelist[q][0] in v:
                            v = v.replace(replacelist[q][0], replacelist[q][1])
            val_l[i] = v
        val_l = list(itertools.chain.from_iterable([x.split() for x in val_l]))
        print(val_l)
        val_l = [x.replace('__', ' ') for x in val_l]
        if separated:
            val_l = first_stub + val_l
        if set(report).issubset(['5', '7']) and separated is False:
            if len(val_l) > 1:
                val_l = [' '.join(val_l[:len(val_l) - len(header_line) + 1])] + val_l[len(val_l) - len(header_line) + 1:]
            if len(val_l) == 1:
                val_l = val_l[len(val_l) - len(header_line) + 1:]
        if val_l == ['DRUG NAME', 'DRUG CODE', 'BUYERSTOTAL GRAMS', 'AVG GRAMS']:
            val_l = ['DRUG NAME', 'DRUG CODE', 'BUYERS', 'TOTAL GRAMS', 'AVG GRAMS']
        if val_l == ['STATE:OKLAHOMA', 'SS', 'ACTIVITY:M', '-', 'MID-LEVEL', 'PRACTITIONERS']:
            val_l = ['STATE:OKLAHOMA', 'BUSINESS', 'ACTIVITY:M', '-', 'MID-LEVEL', 'PRACTITIONERS']
        return val_l
    return val


def find_headvars(str_search):
    header_dict_multi = {}
    for title in list_of_titles:
        if title in str_search:
            # print(title)
            str_search = str_search.replace(title, '').strip()
            header_dict_multi['TITLE'] = title
    match = sorted([(str_search.index(x), x) for x in list(set(headervar_mapping.keys())) if x in str_search], key=lambda x: x[0])
    if match != []:
        for i in range(len(match) - 1):
            header_dict_multi[headervar_mapping[match[i][1]]] = str_search.partition(match[i][1])[2].partition(match[i + 1][1])[0].strip()
        header_dict_multi[headervar_mapping[match[-1:][0][1]]] = str_search.partition(match[-1:][0][1])[2].strip()
    if match != [] or 'TITLE' in header_dict_multi:
        # print(header_dict_multi)
        for key in header_dict_multi:
            if key == 'POP_YR':
                assert is_num_not_nan(header_dict_multi[key]) and header_dict_multi[key].isdigit()
            elif key == 'RUN_DATE':
                assert header_dict_multi[key].replace('/', '').replace(' ', '').isdigit()
            elif key == 'REPORT_PD':
                assert ' TO ' in header_dict_multi[key]
                assert header_dict_multi[key].replace('/', '').replace(' TO ', '').replace(' ', '').isdigit()
            elif key == 'REPORT':
                assert header_dict_multi[key].strip() in ['1', '2', '3', '4', '5', '7']
            elif key == 'STATE':
                print(header_dict_multi[key])
                assert header_dict_multi[key] in statetotallist
            elif key == 'TITLE':
                assert header_dict_multi[key] in list_of_titles
            elif key == 'BUSINESS ACTIVITY':
                # print(header_dict_multi[key])
                if header_dict_multi[key] == 'N-U NARCOTIC TREATMENT':
                    header_dict_multi[key] = 'N-U NARCOTIC TREATMENT PROGRAMS'
                assert header_dict_multi[key] in businessactivities + [x.split(' - ')[1] for x in businessactivities if ' - ' in x]
            elif key == 'PAGE':
                assert header_dict_multi[key].replace(' ', '').isdigit() or header_dict_multi[key].replace(' ', '') == ''
            else:
                assert set(header_dict_multi.keys()).issubset(['DRUG_NAME', 'DRUG_CODE'])
        t, val = list(header_dict_multi.keys()), list(header_dict_multi.values())
        # print('t, val', t, val)
        return t, val
    return None


def return_valid_combined_row(t, val, vala, valb, header_dict, header_line, skip_next_row):
    if (vala, valb) == (['2000', 'GRAMS', 'GRAMS/100K POP'], ['RANK', 'STATE', 'POPULATION', 'TO', 'DATE', 'TO', 'DATE']):
        combine_row = ['RANK', 'STATE', '2000 POP', 'TOTAL GRAMS', 'GRAMS/100K POP']
    elif (vala, valb) == (['', 'NUMBER', 'OF', 'TOTAL GRAMS', 'AVERAGE'], ['DRUG REGISTRANTS SOLD', 'TO', 'GRAM', 'WGT', 'PER']) or (vala, valb) == (['', 'NUMBER', 'OF', 'TOTAL GRAMS', 'AVERAGE'], ['DRUG REGISTRANTS', 'SOLD', 'TO', 'PURCHASE', 'PER']) or ((vala, valb) == (['', 'NUMBER', 'OF', 'TOTAL GRAMS', 'AVERAGE'], []) and header_dict['REPORT'] == ['5']):
        combine_row = ['DRUG NAME', 'DRUG CODE', 'BUYERS', 'TOTAL GRAMS', 'AVG GRAMS']
        if valb == [] or (header_dict['REPORT'] == ['5'] and (header_dict['PAGE'] == ['201'] or header_dict['PAGE'] == ['138'])):
            skip_next_row = 3
            print('skip_next_row4', skip_next_row)
        else:
            skip_next_row = 2
            print('skip_next_row3', skip_next_row)
    elif (vala, valb) == (['U.S. GRAMS / PER 100K:'], ['312,825,210', '577.97', '0.18']):
        combine_row = vala + valb
    elif (vala, valb) == (['U.S. GRAMS / PER 100K:'], ['312,825,210', '182.02', '0.06']):
        combine_row = vala + valb
    elif (vala, valb) == (['312,825,210', '953.32', '0.3'], ['U.S. GRAMS / PER 100K:']):
        combine_row = valb + vala
    else:
        if len([x for x in vala if x != '']) + len([x for x in valb if x != '']) == len(header_line):
            if len([x for x in vala if x == '']) + len([x for x in valb if x == '']) > 0:
                combine_row = []
                i = 0
                for it in vala:
                    if it != '':
                        combine_row.append(it)
                    else:
                        combine_row.append([x for x in valb if x != ''][i])
                        i += 1
                print('combine_row', combine_row)
            else:
                combine_row = vala + valb
        elif ' '.join(vala + valb) in list_of_titles:
            combine_row = vala + valb
        else:
            print('\n', t, val)
            # print(rowdict)
            raise Exception('This row uncategorized (2)!:', t, val)
    return combine_row, skip_next_row
