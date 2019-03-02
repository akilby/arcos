import pdfminer
import glob
import itertools
import sys
import math
import os
import pandas as pd
import numpy as np

from pdfminer.pdfparser import PDFParser
from pdfminer.pdfdocument import PDFDocument
from pdfminer.pdfpage import PDFPage
from pdfminer.pdfpage import PDFTextExtractionNotAllowed
from pdfminer.pdfinterp import PDFResourceManager
from pdfminer.pdfinterp import PDFPageInterpreter
from pdfminer.pdfdevice import PDFDevice
from pdfminer.layout import LAParams
from pdfminer.converter import PDFPageAggregator

from .data.data import (list_of_titles, totallist, statelist, quarterlist,
                        businessactivities, geounitlist, column_titles,
                        headervar_mapping, statetotallist,
                        zip3_state_crosswalk_file)


def build(source_folder, destination_folder):
    globpath = os.path.join(source_folder, '*.pdf')
    Reports_dict = GenerateReports(globpath=globpath)
    report_final_dict, drugset = save_to_disk(Reports_dict, destination_folder)


def GenerateReports(yearlist=None, reportlist=None, globpath=None):
    Reports_dict = {'1': pd.DataFrame(), '2': pd.DataFrame(), '3': pd.DataFrame(), '4': pd.DataFrame(), '5': pd.DataFrame(), '7': pd.DataFrame()}
    if yearlist and reportlist:
        for year in yearlist:
            for report in reportlist:
                print('RUNNING YEAR %s and REPORT %s' % (year, report))
                Report_dict, Report_df, Report = GenerateReport(year, report)
                for key in Report_dict:
                    Reports_dict[key] = pd.concat([Reports_dict[key], Report_dict[key]]).reset_index(drop=True)
    if globpath:
        list_of_files = glob.glob(globpath)
        print(list_of_files)
        for path in list_of_files:
            print('RUNNING PATH %s' % (path))
            Report_dict, Report_df, Report = GenerateReport(path=path)
            for key in Report_dict:
                Reports_dict[key] = pd.concat([Reports_dict[key], Report_dict[key]]).reset_index(drop=True)
    return Reports_dict


def GenerateReport(year=None, report=None, source_folder=None, path=None, start_page=2, end_page=None):
    if not path:
        if year == 2011:
            sep = '-'
        else:
            sep = '_'
        path = os.path.join(source_folder, '%s%srpt%s.pdf' % (year, sep, report))

    year_of_report = [str(x) for x in range(2000, 2018) if str(x) in path.split('/')[-1:][0].split('.pdf')[0]][0]
    if year_of_report in ['2016', '2017']:
        start_page = 1

    Report = ARCOSReport(path, start_page, end_page)
    Report_df = Report.parse_layout()
    Report_dict = {}
    for reportno in set(Report_df['REPORT'].tolist()):
        rdf = Report_df.loc[Report_df['REPORT'] == reportno]
        rdf = subset_to_useful_columns(rdf)
        rdf['YEAR'] = year_of_report
        Report_dict[reportno] = rdf

    return Report_dict, Report_df, Report


class ARCOSReport(object):
    def __init__(self, path, start_page, end_page):
        self.path = path
        lt_objs_container = []
        # Open a PDF file.
        fp = open(path, 'rb')
        # Create a PDF parser object associated with the file object.
        parser = PDFParser(fp)
        # Create a PDF document object that stores the document structure.
        # Password for initialization as 2nd parameter
        self.document = PDFDocument(parser)
        # Check if the document allows text extraction. If not, abort.
        if not self.document.is_extractable:
            raise PDFTextExtractionNotAllowed

        # Create a PDF resource manager object that stores shared resources.
        rsrcmgr = PDFResourceManager()
        # Create a PDF device object.
        self.device = PDFDevice(rsrcmgr)
        # BEGIN LAYOUT ANALYSIS
        # Set parameters for analysis.
        laparams = LAParams()
        # Create a PDF page aggregator object.
        self.device = PDFPageAggregator(rsrcmgr, laparams=laparams)
        # Create a PDF interpreter object.
        self.interpreter = PDFPageInterpreter(rsrcmgr, self.device)
        if end_page is None:
            end_page = 10000000000
        for (pageno, page) in enumerate(PDFPage.create_pages(self.document), start=1):
            if (end_page >= pageno >= start_page):
                self.interpreter.process_page(page)
                lt_objs = self.device.get_result()._objs
                lt_objs_container.append(lt_objs)
                print(pageno, end=' ')
                sys.stdout.flush()
        print('\n')
        self.lt_objs_container = lt_objs_container

    def parse_layout(self):
        skip_next_row = 0
        layouts_list = self.lt_objs_container
        print('TOTAL LAYOUTS TO PROCESS: %s' % len(layouts_list))
        """Function to recursively parse the layout tree."""
        header_dict = {}
        header_line = None
        df_main = pd.DataFrame()
        ltnum = 0
        for col in column_titles:
            header_dict[col] = ''
        for layout in layouts_list:
            print(ltnum, end=' ')
            sys.stdout.flush()
            ltnum += 1
            rowdict = {}
            for lt in layout:
                if isinstance(lt, pdfminer.layout.LTTextBoxHorizontal):
                    for lt_obj_small in lt:
                        assert isinstance(lt_obj_small, pdfminer.layout.LTTextLineHorizontal)
                        coord0, coord1, coord2, coord3 = round(lt_obj_small.bbox[0]), round(lt_obj_small.bbox[1]), round(lt_obj_small.bbox[2]), round(lt_obj_small.bbox[3])
                        if (coord0, coord1, coord2, coord3) == (257, 576, 535, 590) and lt_obj_small.get_text().replace('\n', '').strip() == 'DISTRIBUTION BY STATE IN GRAMS PER 100,000 POPULATION':
                            print(coord0, coord1, coord2, coord3, lt_obj_small.get_text().replace('\n', '').strip())
                            # hand fix
                            print('hand fixing title!!!')
                            coord1 = 575
                        if (coord1, coord3) not in rowdict.keys():
                            rowdict[(coord1, coord3)] = {}
                        rowdict[(coord1, coord3)][(coord0, coord2)] = lt_obj_small.get_text().replace('\n', '').strip()
            keys_sorted = sorted([x for x in rowdict.keys()], key=lambda x: -x[0])
            for rowkey in keys_sorted:
                rowlist = []
                for colkey in sorted([x for x in rowdict[rowkey].keys()], key=lambda x: x[0]):
                    rowlist.append(rowdict[rowkey][colkey])
                if set(header_dict['REPORT']).issubset(['1', '2', '3']):
                    header_line = ['GEO', 'Q1', 'Q2', 'Q3', 'Q4', 'TOTAL']
                if skip_next_row > 0:
                    print('skip_next_row1', skip_next_row)
                    print('skipping row because got joined to previous row', rowkey, rowlist)
                    skip_next_row = skip_next_row - 1
                    print('skip_next_row2', skip_next_row)
                else:
                    t, val = self.categorize_lines(rowlist, header_dict['REPORT'], header_line)
                    # A HAND FIX
                    if val == ['7540', 'METHYLONE (3,4-METHYLENEDIOXY-N-']:
                        val = ['7540', 'METHYLONE (3,4-METHYLENEDIOXY-N-METHYLCATHINONE)']
                        skip_next_row = 1
                        print('need to skip next row')
                    if val == ['2100', 'BARBITURIC ACID DERIVIATIVE OR SALT [PER  21CFR'] or val == ['2100', 'BARBITURIC ACID DERIVIATIVE OR SALT [PER 21CFR']:
                        val = ['2100', 'BARBITURIC ACID DERIVIATIVE OR SALT [PER  21CFR 1308.13(C)(3)]']
                        skip_next_row = 1
                        print('need to skip next row')
                    if val == ['7365', 'DRONABINOL IN AN ORAL SOLUTION IN FDA APPROVED']:
                        val = ['7365', 'DRONABINOL IN AN ORAL SOLUTION IN FDA APPROVED DRUG PRODUCT (SYNDROS - CII)']
                        skip_next_row = 1
                        print('need to skip next row')
                    if set(t).issubset(list(header_dict.keys())):
                        for i in range(len(t)):
                            header_dict[t[i]] = [val[i]]
                    if t == header_line:
                        data_dict = {}
                        for i in range(len(t)):
                            data_dict[t[i]] = [val[i]]
                        df = pd.DataFrame({**header_dict, **data_dict})
                        df_main = pd.concat([df_main, df], axis=0, ignore_index=True)
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
                        t2, val2 = self.categorize_lines(rowlist2, header_dict['REPORT'], header_line)
                        print('next row:', t2, val2)
                        if t2 == 'uncategorized' or t2 == 'blank line':
                            vala, valb = reconcat_list(val, header_dict['REPORT'], header_line), reconcat_list(val2, header_dict['REPORT'], header_line)
                            # print(len(vala), len(valb))
                            print('vala, valb', vala, valb)
                            # print('header_dict', header_dict)
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
                                    print(rowdict)
                                    raise Exception('This row uncategorized (2)!:', t, val)
                            t3, val3 = self.categorize_lines(combine_row, header_dict['REPORT'], header_line)
                            if t3 == header_line:
                                data_dict = {}
                                for i in range(len(t3)):
                                    data_dict[t3[i]] = [val3[i]]
                                df = pd.DataFrame({**header_dict, **data_dict})
                                df_main = pd.concat([df_main, df], axis=0, ignore_index=True)
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
        print('\n')
        return df_main

    def categorize_lines(self, line, report, header_line):
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


def save_to_disk(Reports_dict, folder):
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
