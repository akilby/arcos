"""
To do:

1. Fix bug when running only 2006_rpt1
2. Check for unused code


"""

import glob
import itertools
import os
import sys

import pandas as pd
from arcos import data_dir
from arcos.data.data import col_rename_dict, initialize_header_dict
from arcos.pdfprocess import (layout_to_coordinates_dict, make_row,
                              process_row, rowdict_hand_fixes)
from arcos.postprocess import final_clean
from arcos.utils import pickle_dump
from pdfminer.converter import PDFPageAggregator
from pdfminer.layout import LAParams
from pdfminer.pdfdevice import PDFDevice
from pdfminer.pdfdocument import PDFDocument
from pdfminer.pdfinterp import PDFPageInterpreter, PDFResourceManager
from pdfminer.pdfpage import PDFPage, PDFTextExtractionNotAllowed
from pdfminer.pdfparser import PDFParser


def build(source_folder, destination_folder):
    """
    Main build function: generates reports from every PDF file in
    source_folder, saves intermediate files in a _cached_data
    folder inside the specified destination folder, and if script
    fully runs, saves 6 files (for Reports 1-5 and 7) to the specified
    destination folder
    """
    globpath = os.path.join(source_folder, '*.pdf')
    cachedir = os.path.join(destination_folder, '_cached_data')
    if not os.path.isdir(destination_folder):
        os.mkdir(destination_folder)
    if not os.path.isdir(cachedir):
        os.mkdir(cachedir)
    Reports_dict = GenerateReports(globpath=globpath,
                                   source_folder=source_folder,
                                   save_folder=cachedir)
    pickle_dump(Reports_dict,
                os.path.join(cachedir, 'intermed.pkl'))
    report_final_dict = final_clean(Reports_dict, destination_folder, cachedir)
    return report_final_dict


def GenerateReports(yearlist=None, reportlist=None,
                    source_folder=None, globpath=None,
                    save_folder=None):
    """
    Processes multiple PDFs, either via a passed year/report list,
    or via a globpath. Concatenates multiple years according to
    report number.

    """
    Reports_dict = {'1': pd.DataFrame(),
                    '2': pd.DataFrame(),
                    '3': pd.DataFrame(),
                    '4': pd.DataFrame(),
                    '5': pd.DataFrame(),
                    '7': pd.DataFrame()}
    if yearlist and reportlist:
        check_2019_yearlist(yearlist, source_folder)
        for year in yearlist:
            for report in reportlist:
                print('RUNNING YEAR %s and REPORT %s' % (year, report))
                Report_dict, Report_df, Report = GenerateReport(year, report,
                                                                source_folder)
                Reports_dict = update_Reports_dict(Report_dict, Reports_dict)
    if globpath:
        list_of_files = glob.glob(globpath)

        # Need to handle large 2019 download file
        list_of_files = check_2019_list(list_of_files, source_folder)

        # Start with most recent first, as is most likely to break
        list_of_files = sorted(list_of_files, reverse=True)
        print('Processing files: ', list_of_files)
        for path in list_of_files:
            print('RUNNING PATH %s' % (path))
            Report_dict, Report_df, Report = GenerateReport(
                path=path, save=save_folder)
            Reports_dict = update_Reports_dict(Report_dict, Reports_dict)
    return Reports_dict


def GenerateReport(year=None, report=None, source_folder=None,
                   path=None, save=None):
    """
    Returns all of a single PDF file's report data in a standardized format.
    Some years have reports divided up into multiple PDFs, some are contained
    in one massive PDF. So this produces a dictionary with one item per report
    that was contained in the PDF.

    Saves to disk at the report-year level if save is specified
    """
    if not path:
        if year == 2011:
            sep = '-'
        else:
            sep = '_'
        path = os.path.join(source_folder,
                            '%s%srpt%s.pdf' % (year, sep, report))

    # Some reports have title pages, some do not. This appears to change
    # every time the DEA uploads their data. Should automate a front-page
    # check.

    basep = os.path.splitext(os.path.basename(path))[0]
    [year_of_report] = [str(x) for x in range(2000, 2030) if str(x) in basep]

    Report = ARCOSReport(path)
    Report.year_of_report = year_of_report
    Report_df = Report.process_all_layouts()
    Report_dict = {}

    # Fix geo_units for later years
    if not Report_df.empty:
        Report_df.loc[Report_df.GEO_UNIT == "REGISTRANT ZIP CODE 3",
                      'GEO_UNIT'] = 'ZIP CODE'
        Report_df.loc[Report_df.GEO_UNIT == "STATE NAME",
                      'GEO_UNIT'] = 'STATE'
        for reportno in set(Report_df['REPORT'].tolist()):
            rdf = Report_df.loc[Report_df['REPORT'] == reportno]
            rdf = subset_to_useful_columns(rdf)
            rdf = rdf.rename(columns=col_rename_dict)
            rdf['YEAR'] = year_of_report
            Report_dict[reportno] = rdf

        if save:
            pickle_dump(Report_dict, os.path.join(save, f'{basep}.pkl'))
            for key, val in Report_dict.items():
                save_path = os.path.join(
                    save, f'Report_{key}_{year_of_report}.dta')
                val.to_stata(save_path, write_index=False)
    return Report_dict, Report_df, Report


class ARCOSReport(object):
    """
    Generates an ARCOS report PDF object. Initializing process raw data
    from the PDF into 'layouts.' 1 layout = 1 page.
    """
    def __init__(self, path):
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

        # Process pages
        for (pageno, page) in enumerate(PDFPage.create_pages(self.document)):
            self.interpreter.process_page(page)
            lt_objs = self.device.get_result()._objs
            if ((pageno == 0 and self.first_page_data_bool(lt_objs))
               or pageno > 0):
                lt_objs_container.append(lt_objs)
                print(pageno, end=' ')
            sys.stdout.flush()

        print('\n')
        self.lt_objs_container = lt_objs_container

    def first_page_data_bool(self, lt_objs):
        """
        Returns False if first page is title page and True if first page
        has data that needs processing
        """
        disclaimer_bool = len([x for x in lt_objs
                               if 'DISCLAIMER: Automated Reports and Consolida'
                               in x.get_text()]) > 0
        drug_name_bool = len([x for x in lt_objs
                              if 'DRUG NAME' in x.get_text()]) > 0
        drug_code_bool = len([x for x in lt_objs
                              if 'DRUG CODE' in x.get_text()]) > 0
        diversion_control_bool1 = (('DEPARTMENT OF JUSTICE DRUG ENFORCEMENT AD'
                                   'MINISTRATION, OFFICE OF DIVERSION CONTROL')
                                   in
                                   ''.join([x.get_text() for x in lt_objs])
                                     .replace('\n', ''))
        diversion_control_bool2 = (('DEPARTMENT OF JUSTICE DRUG ENFORCEMENT AD'
                                    'MINISTRATION DEA DIVERSION CONTROL DIVIS')
                                   in
                                   ''.join([x.get_text() for x in lt_objs])
                                     .replace('\n', ''))
        if disclaimer_bool:
            print('Skipping first title page')
            return False
        elif diversion_control_bool1 or diversion_control_bool2:
            print('Skipping first title page')
            return False
        elif drug_name_bool or drug_code_bool:
            assert not disclaimer_bool
            assert not diversion_control_bool1
            assert not diversion_control_bool2
            return True
        else:
            raise Exception('First page is not understood by layout parser')

    def process_all_layouts(self):
        """
        Iterate over layouts, passing information sequentially.
        Because information is ordered/visual, a layout is not
        strictly independent from the layout before it
        """
        (skip_next_row,
            header_line,
            df,
            layout_num,
            partial2019) = (0, None, pd.DataFrame(), 0, False)
        header_dict = initialize_header_dict()
        layouts_list = self.lt_objs_container
        print('TOTAL LAYOUTS TO PROCESS: %s' % len(layouts_list))

        for layout in layouts_list:
            layout_num += 1
            print('LAYOUT: ', layout_num)
            (skip_next_row, partial2019, header_line,
             header_dict, df) = self.process_layout(layout,
                                                    skip_next_row,
                                                    partial2019,
                                                    header_line,
                                                    header_dict,
                                                    df)
        return df

    def process_layout(self, layout, skip_next_row, partial2019,
                       header_line, header_dict, df):
        """
        Upgraded approach uses dictionary_with_offsets_fixed,
        marks two rows as being the same row if
        they are off by only 1 pixel.

        However, prior cleaning used the old, worse, approach.
        So both are run and if they disagree routing throws an error.
        """
        keys_sorted, rowdict = self.layout_to_dictionary(layout)
        keys_sorted2, rowdict2 = self.dictionary_with_offsets_fixed(
            keys_sorted, rowdict)
        try:
            (skip_next_row1, partial20191, header_line1, header_dict1,
                df1) = self.parse_layout_dictionary(keys_sorted,
                                                    rowdict,
                                                    skip_next_row,
                                                    partial2019,
                                                    header_line,
                                                    header_dict,
                                                    df)
            layout_try1 = True
        except Exception:
            layout_try1 = False
        try:
            (skip_next_row2, partial20192, header_line2, header_dict2,
             df2) = self.parse_layout_dictionary(keys_sorted2,
                                                 rowdict2,
                                                 skip_next_row,
                                                 partial2019,
                                                 header_line,
                                                 header_dict,
                                                 df)
            layout_try2 = True
        except Exception:
            layout_try2 = False
        if layout_try1 and not layout_try2:
            return (skip_next_row1, partial20191,
                    header_line1, header_dict1, df1)
        elif layout_try2 and not layout_try1:
            return (skip_next_row2, partial20192,
                    header_line2, header_dict2, df2)
        elif layout_try1 and layout_try2:
            assert df1.equals(df2)
            return (skip_next_row1, partial20191,
                    header_line1, header_dict1, df1)
        else:
            raise Exception('Neither approach to processing layout ran '
                            'without failure')

    def layout_to_dictionary(self, layout):
        """
        Layout processing happens by first translating all text on each page
        to coordinates. Coordinates are strictly respected so as to not use
        bad information from the PDF encoding, which is mostly wrong for ARCOS
        PDF reports
        """
        rowdict = layout_to_coordinates_dict(layout)
        rowdict = rowdict_hand_fixes(rowdict)
        keys_sorted = sorted([x for x in rowdict.keys()], key=lambda x: -x[0])
        rowdict = {key: rowdict[key] for key in keys_sorted}
        # keys_sorted, rowdict = fix_2019_unaligned_rows_report_5_and_7(
        #     rowdict, header_dict)
        return keys_sorted, rowdict

    def dictionary_with_offsets_fixed(self, keys_sorted, rowdict):
        """

        Takes the rowdict dictionary and combines anything where the
        row dimensions are only off by one in one or the other direction

        """

        new_pairs = {}
        update_rowdict = {}
        for t in rowdict.keys():
            search_list = [key for key in rowdict.keys() if key != t]
            for key in search_list:
                if ((t[0] == key[0] and abs(t[1] - key[1]) == 1)
                   or (t[1] == key[1] and abs(t[0] - key[0]) == 1)
                   or (abs(t[0] - key[0]) == 1 and abs(t[1] - key[1]) == 1)):
                    new_key = (round((t[0] + key[0])/2),
                               round((t[1]+key[1])/2))
                    if new_key not in new_pairs:
                        new_pairs[new_key] = [t]
                    else:
                        new_pairs[new_key] = new_pairs[new_key] + [t]
        for ke, va in new_pairs.items():
            L = [val for key, val in rowdict.items() if key in va]
            d = {k: v for d in L for k, v in d.items()}
            d = {key: d[key] for key in sorted(d)}
            update_rowdict[ke] = d

        remove_keys = list(itertools.chain.from_iterable(new_pairs.values()))
        rowdict = {key: val for key, val in rowdict.items()
                   if key not in remove_keys}
        rowdict.update(update_rowdict)
        keys_sorted = sorted([x for x in rowdict.keys()], key=lambda x: -x[0])
        rowdict = {key: rowdict[key] for key in keys_sorted}
        return keys_sorted, rowdict

    def parse_layout_dictionary(self, keys_sorted, rowdict, skip_next_row,
                                partial2019, header_line, header_dict, df):
        """
        Iterates over every row in the layout (that has been converted to a
        dictionary), processes, and updates the dataframe, does something else
        like updating the header, or does nothing if row is useless.
        """
        for rowkey in keys_sorted:
            row = make_row(rowkey, rowdict)
            if (self.year_of_report == "2021"
                    and header_dict['REPORT_PD'] != ''):
                from datetime import datetime
                report_end = (datetime.strptime(header_dict['REPORT_PD'][0]
                              .split('TO')[-1].strip(), "%m/%d/%Y"))
                if report_end.month != 12 or report_end.day != 31:
                    print(
                        f'Partial year {self.year_of_report}')
                    partial2019 = f'OTHER YEAR PARTIAL-{self.year_of_report}'
            if row == ['ZIP CODE', 'QUARTER 1', 'QUARTER 2', 'TOTAL GRAMS']:
                partial2019 = True
                header_line = ['GEO', 'Q1', 'Q2', 'TOTAL']

            if (set(header_dict['REPORT']).issubset(['1', '2', '3'])
               and not partial2019):
                header_line = ['GEO', 'Q1', 'Q2', 'Q3', 'Q4', 'TOTAL']

            if skip_next_row == 0:
                header_dict, header_line, skip_next_row, df = process_row(
                    rowkey, row, header_dict, header_line,
                    keys_sorted, rowdict, skip_next_row, df)
            else:
                print('skip %s next rows' % skip_next_row)
                print('skipping:',
                      rowkey, row)
                skip_next_row = skip_next_row - 1
        return skip_next_row, partial2019, header_line, header_dict, df


def subset_to_useful_columns(df):
    """
    There are columns for each column in every report, but each report only
    needs a subset of those columns, so this drops empty columns

    It also drops Q3 and Q4 in half-year data that has zeros for Q3 and Q4
    """
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
    """
    Appends a new year of Reports to existing report dict -
    each dataframe needs to be concatenated and the index is then
    reset
    """
    for key in Report_dict:
        updated = pd.concat([Reports_dict[key], Report_dict[key]], sort=False)
        updated = updated.reset_index(drop=True)
        Reports_dict[key] = updated
    return Reports_dict


def check_2019_yearlist(yearlist, source_folder):
    if 2019 in yearlist:
        # Need to handle large 2019 download file
        result = check_with_user_about_2019(source_folder)
        if result == 'onboard':
            raise Exception('Pointing to the 2019 onboard file not '
                            'implemented in this subroutine - not sure its '
                            'necessary')


def check_2019_list(list_of_files, source_folder):
    if 'report_yr_2019' in [os.path.split(os.path.splitext(x)[0])[1]
                            for x in list_of_files]:
        result = check_with_user_about_2019(source_folder)
        if result == 'onboard':
            path_2019_onboard = os.path.join(data_dir, 'report_yr_2019.pdf')
            list_of_files = [x if '2019' not in x else path_2019_onboard
                             for x in list_of_files]
        return list_of_files
    return list_of_files


def check_with_user_about_2019(source_folder):
    path_2019_downloaded = os.path.join(source_folder, 'report_yr_2019.pdf')
    path_2019_onboard = os.path.join(data_dir, 'report_yr_2019.pdf')
    s1 = round(os.path.getsize(path_2019_downloaded)/1000000, 1)
    s2 = round(os.path.getsize(path_2019_onboard)/1000000, 1)
    ratio = round(s1/s2)
    if ratio > 2:
        result = user_input(s1, s2, ratio)
    return result


def user_input(s1, s2, ratio):
    result = input('There is something strange about the most recent '
                   'version of report_yr_2019.pdf, where the PDF is very large'
                   ' and much larger than previous versions. \nThe ratio '
                   f'in sizes of the downloaded version ({s1} MB) compared'
                   f' to the original published version ({s2} MB) '
                   f'is {ratio}X. \nAs of 2022/01/06 nothing appears to have '
                   'changed in the underlying data relative to the lightweight'
                   ' (older) PDF, but you might opt to use the '
                   'newest version from the DEA to be safest. For speed, '
                   'the lighter-weight and older version of the 2019 file '
                   'is provided on-board, and you can use this instead.'
                   '\n\tNewest version (safest option) - 1 '
                   '\n\tOnboard version (fastest option) - 2 \n')
    if result == '1':
        return 'newest'
    elif result == '2':
        return 'onboard'
    else:
        print("You must indicate option 1 or 2")
        return user_input(s1, s2, ratio)
