import glob
import itertools
import os
import pickle
import sys

import pandas as pd
from arcos.data.data import col_rename_dict
from arcos.pdfprocess import (initialize_header_dict,
                              layout_to_coordinates_dict, make_row,
                              process_row, rowdict_hand_fixes)
from arcos.postprocess import final_clean
from pdfminer.converter import PDFPageAggregator
from pdfminer.layout import LAParams
from pdfminer.pdfdevice import PDFDevice
from pdfminer.pdfdocument import PDFDocument
from pdfminer.pdfinterp import PDFPageInterpreter, PDFResourceManager
from pdfminer.pdfpage import PDFPage, PDFTextExtractionNotAllowed
from pdfminer.pdfparser import PDFParser


def build(source_folder, destination_folder):
    globpath = os.path.join(source_folder, '*.pdf')
    Reports_dict = GenerateReports(globpath=globpath)
    pickle_dump(Reports_dict,
                os.path.join(source_folder, 'CACHES', 'intermed.pkl'))
    report_final_dict = final_clean(Reports_dict, destination_folder)
    return report_final_dict


def GenerateReports(yearlist=None, reportlist=None,
                    source_folder=None, globpath=None):
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
        for year in yearlist:
            for report in reportlist:
                print('RUNNING YEAR %s and REPORT %s' % (year, report))
                Report_dict, Report_df, Report = GenerateReport(year, report,
                                                                source_folder)
                Reports_dict = update_Reports_dict(Report_dict, Reports_dict)
    if globpath:
        list_of_files = glob.glob(globpath)
        print('Processing files: ', list_of_files)
        for path in list_of_files:
            print('RUNNING PATH %s' % (path))
            Report_dict, Report_df, Report = GenerateReport(path=path)
            Reports_dict = update_Reports_dict(Report_dict, Reports_dict)
    return Reports_dict


def GenerateReport(year=None, report=None, source_folder=None,
                   path=None, start_page=2, end_page=None):
    """
    Returns all of a single PDF file's report data in a standardized format.
    Some years have reports divided up into multiple PDFs, some are contained
    in one massive PDF. So this produces a dictionary with one item per report
    that was contained in the PDF.
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
    [year_of_report] = [str(x) for x in range(2000, 2025) if str(x) in basep]
    if year_of_report in ['2013', '2016', '2017'] or basep == '2012_rpt2':
        start_page = 1

    Report = ARCOSReport(path, start_page, end_page)
    Report_df = Report.process_all_layouts()

    # Fix geo_units for 2018 and 2019
    if year_of_report in ['2018', '2019']:
        Report_df.loc[Report_df.GEO_UNIT == "REGISTRANT ZIP CODE 3",
                      'GEO_UNIT'] = 'ZIP CODE'
        Report_df.loc[Report_df.GEO_UNIT == "STATE NAME",
                      'GEO_UNIT'] = 'STATE'
    Report_dict = {}
    for reportno in set(Report_df['REPORT'].tolist()):
        rdf = Report_df.loc[Report_df['REPORT'] == reportno]
        rdf = subset_to_useful_columns(rdf)
        rdf = rdf.rename(columns=col_rename_dict)
        rdf['YEAR'] = year_of_report
        Report_dict[reportno] = rdf

    # renames here
    return Report_dict, Report_df, Report


class ARCOSReport(object):
    """
    Generates an ARCOS report PDF object. Initializing process raw data
    from the PDF into 'layouts.' 1 layout = 1 page.
    """
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
        for (pageno, page) in enumerate(PDFPage.create_pages(self.document),
                                        start=1):
            if (end_page >= pageno >= start_page):
                self.interpreter.process_page(page)
                lt_objs = self.device.get_result()._objs
                lt_objs_container.append(lt_objs)
                print(pageno, end=' ')
                sys.stdout.flush()
        print('\n')
        self.lt_objs_container = lt_objs_container

    def process_all_layouts(self):
        """
        Iterate over layouts, passing information sequentially.
        Because information is ordered/visual, a layout is not
        strictly independent from the layout before it
        """
        skip_next_row, header_line, df = 0, None, pd.DataFrame()
        header_dict = initialize_header_dict()
        layouts_list = self.lt_objs_container
        print('TOTAL LAYOUTS TO PROCESS: %s' % len(layouts_list))

        layout_num = 0
        partial2019 = False

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
        Layout processing happens by first translating all text on each page
        to coordinates. Coordinates are strictly respected so as to not use
        bad information from the PDF encoding, which is mostly wrong.
        """
        rowdict = layout_to_coordinates_dict(layout)
        rowdict = rowdict_hand_fixes(rowdict)
        keys_sorted = sorted([x for x in rowdict.keys()], key=lambda x: -x[0])
        rowdict = {key: rowdict[key] for key in keys_sorted}
        keys_sorted, rowdict = fix_2019_unaligned_rows_report_5_and_7(
            rowdict, header_dict)
        for rowkey in keys_sorted:
            row = make_row(rowkey, rowdict)

            if row == ['ZIP CODE', 'QUARTER 1', 'QUARTER 2', 'TOTAL GRAMS']:
                partial2019 = True
                header_line = ['GEO', 'Q1', 'Q2', 'TOTAL']

            if (
               set(header_dict['REPORT']).issubset(['1', '2', '3'])
               and not partial2019
               ):
                header_line = ['GEO', 'Q1', 'Q2', 'Q3', 'Q4', 'TOTAL']

            if skip_next_row > 0:
                print('skip %s next rows' % skip_next_row)
                print('skipping:',
                      rowkey, row)
                skip_next_row = skip_next_row - 1
            # elif (header_dict['REPORT'] == ['7'] and
            #       header_dict['RUN_DATE'] == ['07/05/2018'] and
            #       header_dict['REPORT_PD'] == ['01/01/2017 TO 12/31/2017']):
            #     print('Warning: Report 7 for year 2017 is not working after '
            #           '2017 pdf update. Skipping for now.')
            else:
                header_dict, header_line, skip_next_row, df = process_row(
                    rowkey, row, header_dict, header_line,
                    keys_sorted, rowdict, skip_next_row, df)
        return skip_next_row, partial2019, header_line, header_dict, df


def fix_2019_unaligned_rows_report_5_and_7(r, header_dict):
    """
    Could potentially be expanded but only tested on report 5 2019
    """
    if (((header_dict['REPORT'] == ['4']
        and ('ARCOS 3 - REPORT 05' in ' '.join(
               list(itertools.chain.from_iterable(
                   [list(x.values()) for x in r.values()])))))
       or header_dict['REPORT'] == ['5'] or header_dict['REPORT'] == ['7'])
       and '2019' in header_dict['REPORT_PD'][0]):

        rowdict = r.copy()
        replace_pairs = [(x, y) for x in rowdict.keys()
                         for y in rowdict.keys()
                         if x != y
                         and abs(x[0]-y[0]) <= 1
                         and abs(x[1]-y[1]) <= 1
                         and all([abs(x1[0]-y1[0]) >= 5
                                  and abs(x1[1]-y1[1]) >= 5
                                  for x1 in rowdict[x].keys()
                                  for y1 in rowdict[y].keys()])]
        replace_pairs = set([tuple(sorted(x)) for x in replace_pairs])
        for pair in replace_pairs:
            rowdict[pair[0]] = {**rowdict[pair[0]], **rowdict[pair[1]]}
            del rowdict[pair[1]]
        keys_sorted = sorted([x for x in rowdict.keys()], key=lambda x: -x[0])
        return keys_sorted, rowdict
    keys_sorted = sorted([x for x in r.keys()], key=lambda x: -x[0])
    return keys_sorted, r


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
        updated = pd.concat([Reports_dict[key], Report_dict[key]], sort=False)
        updated = updated.reset_index(drop=True)
        Reports_dict[key] = updated
    return Reports_dict


def pickle_read(readfile):
    with open(readfile, 'rb') as picklefile:
        thing = pickle.load(picklefile)
    return thing


def pickle_dump(thing, writefile):
    with open(writefile, 'wb') as picklefile:
        pickle.dump(thing, picklefile)
