import itertools
import re
import sys
from datetime import datetime

import dateutil
import numpy as np
import pandas as pd
import pdfminer
from arcos.data.data import (businessactivities, geounitlist,
                             headervar_mapping, list_of_titles, quarterlist,
                             statelist, statelist_2018_2019, statetotallist)
from arcos.data.handfixes import (fix_weird_combines,
                                  list_of_vala_valb_for_weird_combines,
                                  ltcd_hand_fix, post_categorize_hand_fix)


def process_row(rowkey, row, header_dict, header_line,
                keys_sorted, rowdict, skip_next_row, df_main):
    t, val = categorize_lines(row, header_dict['REPORT'], header_line)
    if not df_main.empty:
        t, val = categorize_recent_reports(
            t, val, row, header_dict['REPORT'], header_line, df_main)
    val, skip_next_row = post_categorize_hand_fix(val, skip_next_row)
    header_dict = update_header_dict(t, val, header_dict)
    if t == header_line:
        df_main = add_line_df(t, val, header_dict, df_main)
    if t == 'header line':
        header_line = val
    if t == 'uncategorized':
        if check_dangling_from_previous_line(keys_sorted, header_dict,
                                             header_line, rowdict, rowkey,
                                             row, assert_type=['GEO_UNIT']):
            return header_dict, header_line, skip_next_row, df_main
        print(t, val)
        print('skip_next_row0', skip_next_row)
        print('\n')
        print('row:', t, val, rowdict)
        key_next = keys_sorted[keys_sorted.index(rowkey) + 1]
        rowlist2 = []
        for colkey2 in sorted([x for x in rowdict[key_next].keys()],
                              key=lambda x: x[0]):
            rowlist2.append(rowdict[key_next][colkey2])
        t2, val2 = categorize_lines(
            rowlist2, header_dict['REPORT'], header_line)
        print('next row:', t2, val2)
        if t2 == 'uncategorized' or t2 == 'blank line':
            t3, val3, skip_next_row, need_to_check_coords = check_two_lines(
                t, val, val2, header_dict, header_line, skip_next_row)
            if t3 == header_line:
                if need_to_check_coords:
                    # assert (max(rowdict[rowkey].keys())
                    #         < max(rowdict[key_next].keys()))
                    coord_map = sorted([(x[1], x[0]) for x in
                                        list({**rowdict[rowkey],
                                              **rowdict[key_next]}.items())
                                        if x[1] in val3],
                                       key=lambda x: val3.index(x[0]))
                    # Check 1: could just be slightly misaligned, in which
                    # case the horizontal coordinates will be in order
                    mask1 = (sorted([x[1] for x in coord_map])
                             == [x[1] for x in coord_map])
                    # Check 2: could be on an entirely new line, as overflow so
                    # should be one row lower, and positioned far to the left,
                    # starting in first half of page
                    line_vert_dist = calculate_line_vertical_distance(
                        keys_sorted)
                    vertdist = (rowkey[0]-key_next[0]
                                + rowkey[1]-key_next[1])/2
                    mask2 = False
                    if line_vert_dist - 1 <= vertdist <= line_vert_dist + 1:
                        mask2 = (min([x[0] for x
                                      in list(rowdict[key_next].keys())])
                                 < round(max([x[1][1] for x in coord_map])/2))
                    assert mask1 or mask2
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
        elif check_2018_missing_types(val, val2)[0]:
            check, vala, valb = check_2018_missing_types(val, val2)
            print(check, vala, valb)
            print('DEBUG WORKED')
            t, val = categorize_lines(vala, header_dict['REPORT'], header_line)
            header_dict = update_header_dict(t, val, header_dict)
            if t == header_line:
                df_main = add_line_df(t, val, header_dict, df_main)
            if t == 'header line':
                header_line = val
            t, val = categorize_lines(valb, header_dict['REPORT'], header_line)
            header_dict = update_header_dict(t, val, header_dict)
            if t == header_line:
                df_main = add_line_df(t, val, header_dict, df_main)
            if t == 'header line':
                header_line = val
            print('last lines:')
            print(df_main[-13:])
            skip_next_row = 1
        elif (t2 == header_line
                and val == ['DRONABINOL IN AN ORAL SOLUTION',
                            'IN', 'FDA', 'APPROVED', 'DRUG']
                and val2 == ['PRODUCT (SYNDROS -       CII)',
                             '7365', '13', '5.25', '0.4']):
            # This is a weird one that doesn't get caught because the next
            # line is fully valid
            valfinal = ['DRONABINOL IN AN ORAL SOLUTION IN FDA APPROVED'
                        '  DRUG PRODUCT (SYNDROS -       CII)'] + val2[1:]
            df_main = add_line_df(t2, valfinal, header_dict, df_main)
            if skip_next_row == 0:
                skip_next_row = 1
        else:
            # breakpoint()
            raise Exception('This row uncategorized (1)!:', t, val, rowdict)
    return header_dict, header_line, skip_next_row, df_main


def categorize_lines(line, report, header_line):
    print('categorize lines current line', line)
    print('categorize lines current report number', report)
    print('categorize lines current header_line', header_line)
    line = reconcat_list(line, report, header_line)
    str_search = ' '.join(line)
    print('categorize lines str_search', str_search)
    if not report:
        if find_headvars(str_search):
            t, val = find_headvars(str_search)
        elif str_search == '' or line == [''] or line == []:
            t, val = 'blank line', line
        else:
            t, val = 'uncategorized', line
    elif set(report).issubset(['1', '2', '3']):
        test1 = [[x for x in line if x not in q] for q in quarterlist
                 if len([x for x in line if x in q]) == len(q)]
        test1 = [x for x in test1 if len(x) == min([len(y) for y in test1])]
        test1 = [x[0] for x in test1 if len(test1) == 1 and len(test1[0]) == 1]
        if (set(test1).issubset(set(geounitlist))
           and test1 != []
           and any([len([x for x in line if x in q]) == len(q)
                    for q in quarterlist])):
            t, val = ['GEO_UNIT'], test1
        elif (len(line) == len(header_line)
              and all([is_num_not_nan(x) for x in line[1:]])
              and (line[0:1][0] in statetotallist or line[0:1][0].isdigit())):
            t, val = header_line, line
        elif (len(line) == (len(header_line) + 1)
              and sum([is_num_not_nan(x) for x in line[1:]]) == 5
              and len([x for x in line[1:] if x == '']) == 1
              and line[0:1][0] in statetotallist):
            t, val = header_line, [x for x in line if x != '']
        elif (str_search == '' or line == ['']
              or line == ['SUBSTANCE'] or line == ['2'] or line == ['J']):
            t, val = 'blank line', line
        elif ((len(line) < len(header_line))
              and len(list(itertools.chain.from_iterable(
                [x.split(' ') for x in line]))) == 6
              and all([is_num_not_nan(x)
                       for x in
                       list(itertools.chain.from_iterable(
                        [x.split(' ') for x in line]))[1:]])):
            line_new = list(
                itertools.chain.from_iterable([x.split(' ') for x in line]))
            t, val = header_line, line_new
            print('last_cat')
        elif find_headvars(str_search):
            t, val = find_headvars(str_search)
        elif (len(line) == 3 and line[0].isdigit()
              and line[2].isdigit() and line[1].lower() == 'of'):
            t, val = 'blank line', line
        elif len(line) == 1 and line[0] in statelist:
            str_search = 'STATE: %s' % str_search
            if find_headvars(str_search):
                t, val = find_headvars(str_search)
        elif len(line) >= 5 and is_time(str_search):
            t, val = 'blank line', line
        elif len(line) == 6 and is_time(' '.join(line[:3] + line[4:])):
            t, val = 'blank line', line
        else:
            # print('str_search', str_search)
            t, val = 'uncategorized', line
    elif set(report).issubset(['4', '5', '7']):
        if (report == ['7']
           and str_search == ('REPORTING PERIOD: January 1, 2017 '
                              'to December 31,2017')):
            str_search = 'DATE RANGE: 01/01/2017 TO 12/31/2017'
        test1 = (sum([line == q for q in quarterlist]) == 1)
        if test1:
            t, val = 'header line', line
        elif (str_search ==
              'RANK STATE POPULATION GRAMS TO DATE GRAMS/100K POP. TO DATE'):
            (t, val) = ('header line',
                        ['RANK', 'STATE', '2000 POP',
                         'TOTAL GRAMS', 'GRAMS/100K POP'])
        elif (report == ['4'] and len(line) == len(header_line)
              and line[0].isdigit() and line[1] in statetotallist
              and all([is_num_not_nan(x) for x in line[2:]])):
            t, val = header_line, line
        elif (report == ['4'] and len(line) == 4
              and line[0] in ['U.S. GRAMS / PER 100K:',
                              'U.S. GRAMS / PER 100K POPULATION',
                              'U.S. GRAMS/100K POP:', 'TOTAL']
              and all([is_num_not_nan(x) for x in line[2:]])):
            t, val = header_line, ['N/A'] + line
        elif (report == ['4'] and len(line) == 4
              and line[0] in ['US TOTAL']
              and all([is_num_not_nan(x) for x in line[2:]])
              and line[1] == '312,825,210'):
            t, val = header_line, ['N/A'] + line
        elif (set(report).issubset(['5', '7'])
              and len(line) == len(header_line)
              and line[2].replace(',', '').isdigit()
              and all([is_num_not_nan(x) for x in line[3:]])):
            t, val = header_line, line
        elif (set(report).issubset(['5', '7'])
              and len(line) == (len(header_line) + 1)
              and line[2].replace(',', '').isdigit()
              and all([any([is_num_not_nan(x), x == '']) for x in line[3:]])
              and '' in line):
            t, val = header_line, [x for x in line if x != '']
        elif find_headvars(str_search):
            t, val = find_headvars(str_search)
        elif (str_search == '' or line == [''] or line == ['SUBSTANCE']
              or line == ['PROGRAMS'] or line == ['J']):
            t, val = 'blank line', line
        elif (len(line) == 3 and line[0].isdigit()
              and line[2].isdigit() and line[1].lower() == 'of'):
            t, val = 'blank line', line
        elif (len(line) == 1 and len(line[0].split()) == 3):
            line = line[0].split()
            if (line[0].isdigit()
                    and line[2].isdigit()
                    and line[1].lower() == 'of'):
                t, val = 'blank line', line
        elif report == ['5'] and line == ['17,627,063.62']:
            t, val = 'blank line', line
        elif len(line) >= 3 and is_time(str_search):
            t, val = 'blank line', line
        elif report == ['7'] and str_search == ' ENTIRE UNITED STATES':
            t, val = 'blank line', line
        else:
            # print('str_search', str_search)
            t, val = 'uncategorized', line
    return t, val


def check_dangling_from_previous_line(keys_sorted, header_dict, header_line,
                                      rowdict, rowkey, row,
                                      assert_type=None):
    """
    Checks if row is dangling from previous line. Probably only necessary for
    a GEO_UNIT type line in 2018 and 2019
    """
    mask = (header_dict['REPORT_PD'] == ['01/01/2019 TO 12/31/2019']
            or header_dict['REPORT_PD'] == ['01/01/2018 TO 12/31/2018'])
    if not mask:
        return False
    rowl = []
    key_prev = keys_sorted[keys_sorted.index(rowkey) - 1]
    for c in sorted([x for x in rowdict[key_prev].keys()], key=lambda x: x[0]):
        rowl.append(rowdict[key_prev][c])
    t1, val1 = categorize_lines(rowl + row, header_dict['REPORT'], header_line)
    t2, val2 = categorize_lines(rowl, header_dict['REPORT'], header_line)
    mask1 = (t1 == assert_type) if assert_type else True
    mask2 = (t1, val1) == (t2, val2)
    return mask1 & mask2 & mask


def check_2018_missing_types(vala, valb):
    check = False
    if (
        (len(vala) == 5 and all([is_num_not_nan(x) for x in vala])) and
        (len(valb) == 1 and valb[0] in statelist)
       ):
        vala = ['TOTAL'] + vala
        valb = ['STATE:'] + valb
        check = True
    elif (
        (len(vala) == 5 and all([is_num_not_nan(x) for x in vala])) and
        (len(valb) == 2 and valb[1] in statelist)
       ):
        vala = ['TOTAL'] + vala
        valb = valb
        check = True
    return check, vala, valb


def categorize_recent_reports(t, val, row, report, header_line, df_main):
    """
    Starting in 2018 drug names were included in the total lines for report 2
    Use the existing drug names from report 1 to categorize these lines
    """
    if (report == ['2'] or report == ['3'] or report == ['4']):
        drugtotallist = list((df_main.DRUG_CODE + ' - '
                             + df_main.DRUG_NAME + " - Total")
                             .value_counts().index)
        drugtotallist2 = {' '.join(x.replace('-', '').split()): x
                          for x in drugtotallist}
    if t == 'uncategorized' and (report == ['2'] or report == ['3']):
        if (len(row) == len(header_line)
           and all([is_num_not_nan(x) for x in row[1:]])
           and (row[0:1][0] in drugtotallist)):
            (t, val) = (header_line, row)
            return t, val
        elif ((len(row) - len(header_line) == 1 and '' in row)
              or (len(row) == len(header_line))):
            row = [x for x in row if x != '']
            matches = [x[:len(row[0:1][0])] for x in drugtotallist
                       if x.startswith(row[0:1][0])]
            if (len(row) == len(header_line)
               and all([is_num_not_nan(x) for x in row[1:]])
               and len(matches) == 1 and len(matches[0]) > 20):
                (t, val) = (header_line, row)
                return t, val
        # Reconcatenate and do a more complicated match to handle weird cases
        # with dashes
        reconcat = [' '.join(x.replace('-', '').split())
                    for x in row if x.replace('-', '') != '']
        if len(reconcat) == len(header_line):
            matches = [x[:len(reconcat[0:1][0])] for x in drugtotallist2.keys()
                       if x.startswith(reconcat[0:1][0])]
            if (all([is_num_not_nan(x) for x in reconcat[1:]])
               and len(matches) == 1 and len(matches[0]) >= 15):
                (t, val) = (header_line, [x if x not in drugtotallist2
                                          else drugtotallist2[x]
                                          for x in reconcat])
                return t, val
            elif (all([is_num_not_nan(x) for x in reconcat[1:]])
                  and len(matches) == 1 and len(matches[0]) < 15):
                raise Exception("Need to verify "
                                "length by hand in categorize_recent_reports")
    if t == 'uncategorized' and (report == ['4']):
        newline = list(itertools.chain.from_iterable(
            [x.split(' ') if x not in drugtotallist else [x] for x in row]))
        reconcat = [' '.join(x.replace('-', '').split())
                    for x in row if x.replace('-', '') != '']
        reconcat = reconcat[:1] + list(
            itertools.chain.from_iterable([x.split() for x in reconcat[1:]]))
        reconcat = [x if x not in drugtotallist2
                    else drugtotallist2[x]
                    for x in reconcat]
        if len(newline) > 4 and len(reconcat) == 4:
            newline = reconcat
        if (report == ['4'] and len(newline) == 4
           and newline[0] in drugtotallist
           and all([is_num_not_nan(x) for x in newline[1:]])
           and newline[1] == '312,825,210'):
            t, val = header_line, ['N/A'] + newline
            return t, val

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


def check_two_lines(t, val, val2, header_dict, header_line, skip_next_row):
    vala = reconcat_list(val, header_dict['REPORT'], header_line)
    valb = reconcat_list(val2, header_dict['REPORT'], header_line)
    print('vala, valb', vala, valb)
    print('header_dict', header_dict)
    (combine_row,
        skip_next_row, need_to_check_coords) = return_valid_combined_row(
        val, vala, valb, header_dict, header_line, skip_next_row)
    t3, val3 = categorize_lines(
        combine_row, header_dict['REPORT'], header_line)
    if (t3 == 'uncategorized'
        and (header_dict['REPORT_PD'] == ['01/01/2019 TO 12/31/2019']
             or header_dict['REPORT_PD'] == ['01/01/2018 TO 12/31/2018'])):
        (combine_row,
            skip_next_row, need_to_check_coords) = return_valid_combined_row(
            val, valb, vala, header_dict, header_line, skip_next_row)
        t3, val3 = categorize_lines(
            combine_row, header_dict['REPORT'], header_line)
    return t3, val3, skip_next_row, need_to_check_coords


# def fix_2019_unaligned_rows_report_5_and_7(r, header_dict):
#     """
#     Could potentially be expanded but only tested on report 5 2019
#     """
#     if (((header_dict['REPORT'] == ['4']
#         and ('ARCOS 3 - REPORT 05' in ' '.join(
#                list(itertools.chain.from_iterable(
#                    [list(x.values()) for x in r.values()])))))
#        or header_dict['REPORT'] == ['5'] or header_dict['REPORT'] == ['7'])
#        and '2019' in header_dict['REPORT_PD'][0]):
#
#         rowdict = r.copy()
#         replace_pairs = [(x, y) for x in rowdict.keys()
#                          for y in rowdict.keys()
#                          if x != y
#                          and abs(x[0]-y[0]) <= 1
#                          and abs(x[1]-y[1]) <= 1
#                          and all([abs(x1[0]-y1[0]) >= 5
#                                   and abs(x1[1]-y1[1]) >= 5
#                                   for x1 in rowdict[x].keys()
#                                   for y1 in rowdict[y].keys()])]
#         replace_pairs = set([tuple(sorted(x)) for x in replace_pairs])
#         for pair in replace_pairs:
#             rowdict[pair[0]] = {**rowdict[pair[0]], **rowdict[pair[1]]}
#             del rowdict[pair[1]]
#         keys_sorted = sorted([x for x in rowdict.keys()], key=lambda x: -x[0])
#         return keys_sorted, rowdict
#     keys_sorted = sorted([x for x in r.keys()], key=lambda x: -x[0])
#     return keys_sorted, r


def make_row(rowkey, rowdict):
    row = []
    for colkey in sorted([x for x in rowdict[rowkey].keys()],
                         key=lambda x: x[0]):
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
    """
    If two cells are joined together in one box, tries to separate them

    First comes up with which cells have spaces in them but are one valid cell

    """
    separated = False
    for st in ['DRUG ENFORCEMENT ADMINISTRATION, OFFICE OF DIVERSION CONTROL',
               'DRUG ENFORCEMENT ADMINISTRATION', 'DEPARTMENT OF JUSTICE']:
        val = [x.replace(st, '').strip() for x in val if x != st]
    if len(val) == 1:
        if (('--' in val[0]
            and len([x for x in val[0] if x == '-']) == len(val[0]))
           or val[0] == '-' or val[0] == '\x1a'):
            print('DELETING, %s' % val[0])
            val = ['']
    if all([type(x) is str for x in val]):
        if set(report).issubset(['5', '7']) and len(val) != 1:
            separated = True
            first_stub = val[:1]
            val = val[1:]
        # One report, 2019, has weird spacing and dashes in the statetotallist
        r = ' '.join(val).replace(' ', '').replace('-', '')
        altdict = {x.replace(' ', '').replace('-', ''): x
                   for x in statetotallist if '-' in x}
        altlist = list(altdict.keys())
        m = [x for x in altlist if r.startswith(x)]
        if len(m) == 1:
            f = r[len(m[0]):]
            for i in range(len(val)):
                if ''.join(val[i:]) == f:
                    pos_split = i
            reconcatted_row = [altdict[m[0]]] + val[pos_split:]
            val = reconcatted_row

        # Make a list of valid string replacements where spaces are replaced
        # by underscores
        replacelist = [(x, x.replace(' ', '__'))
                       for x in statetotallist + geounitlist
                       + list(itertools.chain.from_iterable(quarterlist))]
        replacelist = sorted([(x[0], x[1]) for x in replacelist],
                             key=lambda x: len(x[1]), reverse=True)
        replacelist = replacelist + [
            ('QUARTER__2ND__QUARTER', 'QUARTER 2ND__QUARTER'),
            ('QUARTER__3RD__QUARTER', 'QUARTER 3RD__QUARTER'),
            ('QUARTER__4TH__QUARTER', 'QUARTER 4TH__QUARTER')]
        val_l = [val[i] for i in range(len(val))]
        # Iterate through the row and replace with the replacelist
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
            if (len(val_l) > 1 and not (len(val_l) == 3
               and val_l[1].lower() == 'of' and val_l[0].isdigit()
               and val_l[2].isdigit()) and not (val_l == ['DATE', 'RANGE:'])):
                val_l = ([' '.join(val_l[:len(val_l) - len(header_line) + 1])]
                         + val_l[len(val_l) - len(header_line) + 1:])
            if len(val_l) == 1:
                val_l = val_l[len(val_l) - len(header_line) + 1:]
        if val_l == ['DRUG NAME', 'DRUG CODE',
                     'BUYERSTOTAL GRAMS', 'AVG GRAMS']:
            val_l = ['DRUG NAME', 'DRUG CODE', 'BUYERS',
                     'TOTAL GRAMS', 'AVG GRAMS']
        if val_l == ['STATE:OKLAHOMA', 'SS', 'ACTIVITY:M', '-',
                     'MID-LEVEL', 'PRACTITIONERS']:
            val_l = ['STATE:OKLAHOMA', 'BUSINESS', 'ACTIVITY:M', '-',
                     'MID-LEVEL', 'PRACTITIONERS']
        return val_l
    return val


def find_headvars(str_search):
    print('str_search', str_search)
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
        if ('REPORT_PD', 'REPORT 3') in header_dict_multi.items():
            assert header_dict_multi['REPORT'] == '3'
            del header_dict_multi['REPORT_PD']
            print('DELETED', 'REPORT_PD', 'REPORT 3')
    if match != [] or 'TITLE' in header_dict_multi:
        # print(header_dict_multi)
        for key in header_dict_multi:
            if key == 'POP_YR':
                assert is_num_not_nan(header_dict_multi[key]) and header_dict_multi[key].isdigit()
            elif key == 'RUN_DATE':
                assert header_dict_multi[key].replace('/', '').replace(' ', '').isdigit()
            elif key == 'REPORT_PD':
                print(header_dict_multi)
                print(str_search)
                assert (((' TO ' in header_dict_multi[key]) and
                        (header_dict_multi[key].replace('/', '').replace(' TO ', '').replace(' ', '').isdigit())) or
                        (header_dict_multi[key] == '' and str_search == 'DATE RANGE:'))
            elif key == 'REPORT':
                header_dict_multi[key] = header_dict_multi[key].strip().replace(' by GRAMS', '')
                assert (int(header_dict_multi[key])
                        in [1, 2, 3, 4, 5, 7])
                header_dict_multi[key] = str(int(header_dict_multi[key]))
                # assert header_dict_multi[key].strip() in ['1', '2', '3', '4', '5', '7']
            elif key == 'STATE':
                print(header_dict_multi[key])
                try:
                    assert header_dict_multi[key] in statetotallist
                except AssertionError:
                    s20182019 = statelist_2018_2019()
                    assert header_dict_multi[key] in s20182019.keys()
                    header_dict_multi[key] = s20182019[header_dict_multi[key]]
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
                if 'DRUG_CODE' in header_dict_multi.keys() and 'DRUG_NAME' not in header_dict_multi.keys():
                    if ' - ' in header_dict_multi['DRUG_CODE']:
                        COD, NAM = header_dict_multi['DRUG_CODE'].split(' - ')
        try:
            header_dict_multi['DRUG_CODE'] = COD
            header_dict_multi['DRUG_NAME'] = NAM
        except NameError:
            print('', end='')
        if re.search(r'\s*REPORT\s+[0-9]*', str_search):
            reportno = re.findall(r'\s*REPORT\s+[0-9]*', str_search)
            assert len(reportno) == 1
            reportno = reportno[0].replace('REPORT', '').strip()
            reportno = str(int(reportno))
            if 'REPORT' in header_dict_multi.keys():
                assert header_dict_multi['REPORT'] == reportno
            else:
                header_dict_multi['REPORT'] = reportno
        t, val = list(header_dict_multi.keys()), list(header_dict_multi.values())
        # print('t, val', t, val)
        print('HEADER FOUND:', 't: ', t, 'val:', val)
        return t, val
    else:
        lin = str_search.split(' ')
        if len(lin) == 3:
            if isdatetime(lin[0], '%M/%d/%Y') and lin[1] == 'TO' and isdatetime(lin[2], '%M/%d/%Y'):
                t = ['REPORT_PD']
                val = [str_search]
                print('t: ', t, 'val:', val)
                return t, val
    return None


def isdatetime(usestr, datestr='%M/%d/%Y'):
    try:
        datetime.strptime(usestr, datestr)
        return True
    except ValueError:
        return False


def is_time(str_search):
    try:
        pd.to_datetime(str_search)
        return True
    except dateutil.parser._parser.ParserError:
        return False
    except ValueError:
        return False


def return_valid_combined_row(val, vala, valb,
                              header_dict, header_line, skip_next_row):
    need_to_check_coords = False
    if (vala, valb) == (['2000', 'GRAMS', 'GRAMS/100K POP'],
                        ['RANK', 'STATE', 'POPULATION', 'TO',
                         'DATE', 'TO', 'DATE']):
        combine_row = ['RANK', 'STATE', '2000 POP',
                       'TOTAL GRAMS', 'GRAMS/100K POP']
    elif (
        (vala, valb) == (['', 'NUMBER', 'OF', 'TOTAL GRAMS', 'AVERAGE'],
                         ['DRUG REGISTRANTS SOLD', 'TO', 'GRAM', 'WGT', 'PER'])
        or (vala, valb) == (['', 'NUMBER', 'OF', 'TOTAL GRAMS', 'AVERAGE'],
                            ['DRUG REGISTRANTS', 'SOLD',
                             'TO', 'PURCHASE', 'PER'])
        or ((vala, valb) == (['', 'NUMBER', 'OF', 'TOTAL GRAMS', 'AVERAGE'],
                             [])
            and header_dict['REPORT'] == ['5'])):
        combine_row = ['DRUG NAME', 'DRUG CODE',
                       'BUYERS', 'TOTAL GRAMS', 'AVG GRAMS']
        if (valb == [] or (header_dict['REPORT'] == ['5']
                           and (header_dict['PAGE'] == ['201']
                                or header_dict['PAGE'] == ['138']))):
            skip_next_row = 3
            print('skip_next_row4', skip_next_row)
        else:
            skip_next_row = 2
            print('skip_next_row3', skip_next_row)
    elif (vala, valb) == (['U.S. GRAMS / PER 100K:'],
                          ['312,825,210', '577.97', '0.18']):
        combine_row = vala + valb
    elif (vala, valb) == (['U.S. GRAMS / PER 100K:'],
                          ['312,825,210', '182.02', '0.06']):
        combine_row = vala + valb
    elif (vala, valb) == (['U.S. GRAMS / PER 100K:'],
                          ['312,825,210', '953.32', '0.3']):
        combine_row = vala + valb
    elif (vala, valb) == (['312,825,210', '953.32', '0.3'],
                          ['U.S. GRAMS / PER 100K:']):
        combine_row = valb + vala
    elif (vala, valb) in list_of_vala_valb_for_weird_combines:
        combine_row = fix_weird_combines(vala, valb)
    elif (vala, valb) == (['1724', '-', 'METHYLPHENIDATE',
                           '(DL;D;L;ISOMERS)', '-', 'Total'],
                          ['312,825,210', '17,820,740.31', '5,696.71']):
        combine_row = ['TOTAL'] + valb
    else:
        need_to_check_coords = True
        if (
            len([x for x in vala if x != ''])
            + len([x for x in valb if x != '']) == len(header_line)
           ):
            if (
                len([x for x in vala if x == ''])
                + len([x for x in valb if x == '']) > 0
               ):
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
            print('\n', val)
            # print(rowdict)
            raise Exception('This row uncategorized (2)!:', val)
    return combine_row, skip_next_row, need_to_check_coords


def rowdict_hand_fixes(rowdict):
    if (
        (72, 84) in rowdict.keys()
        and (71, 83) in rowdict.keys()
        and (71, 84) in rowdict.keys()
       ):
        if (
            (rowdict[(72, 84)] == {(468, 505): '21,503.11',
                                   (582, 619): '18,789.59'})
            and (rowdict[(71, 83)] == {(52, 66): '336',
                                       (240, 277): '20,637.06',
                                       (354, 391): '17,647.68'})
            and (rowdict[(71, 84)] == {(696, 733): '78,577.44'})
           ):
            print('Warning: Hand fixing the rowdict')
            rowdict[(71, 83)] = {**rowdict[(71, 83)],
                                 **rowdict[(72, 84)],
                                 **rowdict[(71, 84)]}
            del rowdict[(71, 84)]
            del rowdict[(72, 84)]
    if ((522, 536) in rowdict.keys() and (523, 535) in rowdict.keys()):
        if (
            rowdict[(522, 536)] == {(533, 591): 'TOTAL GRAMS'}
            and rowdict[(523, 535)] == {(109, 159): 'DRUG NAME',
                                        (253, 300): 'DRUG CODE',
                                        (352, 487): 'NUMBER OF '
                                                    'REGISTRANT SOLD TO',
                                        (657, 706): 'AVG GRAMS'}
           ):
                rowdict[(523, 535)] = {(109, 159): 'DRUG NAME',
                                       (253, 300): 'DRUG CODE',
                                       (352, 487): 'NUMBER OF '
                                                   'REGISTRANT SOLD TO',
                                       (533, 591): 'TOTAL GRAMS',
                                       (657, 706): 'AVG GRAMS'}
                del rowdict[(522, 536)]
    if ((140, 152) in rowdict.keys() and (139, 152) in rowdict.keys()):
        if (
            rowdict[(139, 152)] == {(533, 591): 'TOTAL GRAMS'}
            and rowdict[(140, 152)] == {(109, 159): 'DRUG NAME',
                                        (253, 300): 'DRUG CODE',
                                        (352, 487): 'NUMBER OF '
                                                    'REGISTRANT SOLD TO',
                                        (657, 706): 'AVG GRAMS'}
           ):
                rowdict[(140, 152)] = {(109, 159): 'DRUG NAME',
                                       (253, 300): 'DRUG CODE',
                                       (352, 487): 'NUMBER OF '
                                                   'REGISTRANT SOLD TO',
                                       (533, 591): 'TOTAL GRAMS',
                                       (657, 706): 'AVG GRAMS'}
                del rowdict[(139, 152)]
    if ((282, 296) in rowdict.keys() and (267, 281) in rowdict.keys()):
        if (
            rowdict[(282, 296)] ==
            {(20, 321): 'DRONABINOL IN AN ORAL SOLUTION IN FDA APPROVED  DRUG'}
            and rowdict[(267, 281)] ==
            {(20, 152): 'PRODUCT (SYNDROS -       CII)'}
           ):
            rowdict[(266, 280)] = {(20, 321): 'DRONABINOL IN AN ORAL SOLUTION '
                                              'IN FDA APPROVED DRUG PRODUCT '
                                              '(SYNDROS - CII)',
                                   (391, 414): '7365',
                                   (594, 606): '13',
                                   (663, 682): '5.25',
                                   (740, 754): '0.4'}
            del rowdict[(282, 296)]
            del rowdict[(267, 281)]
    return rowdict


def calculate_line_vertical_distance(keys_sorted):
    li = [keys_sorted[i][0]-keys_sorted[i+1][0]
          for i in range(len(keys_sorted)-1)]
    return max(set(li), key=li.count)
