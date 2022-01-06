# import itertools

list_of_vala_valb_for_weird_combines = [
  (['567,865.82'],
   ['9652', '-', 'OXYMORPHONE', '-', 'Total', '312,825,210', '181.53']),
  (['3,628,686.35'],
   ['9780', '-', 'TAPENTADOL', '-', 'Total', '312,825,210', '1,159.97']),
  (['6,731.76'],
   ['1105', '-', 'METHAMPHETAMINE', '-', 'Total', '312,825,210', '2.15']),
  (['9,708,996.30'],
   ['1205', '-', 'LISDEXAMFETAMINE', '-', 'Total', '312,825,210', '3,103.65'])
]


def fix_weird_combines(vala, valb):
    if (vala, valb) == (['3,628,686.35'],
                        ['9780', '-', 'TAPENTADOL', '-', 'Total',
                         '312,825,210', '1,159.97']):
        return ['9780 - TAPENTADOL - Total'] + valb[5:6] + vala + valb[-1:]
    elif (vala, valb) == (['6,731.76'],
                          ['1105', '-', 'METHAMPHETAMINE', '-', 'Total',
                           '312,825,210', '2.15']):
        return (['1105 - METHAMPHETAMINE - Total']
                + valb[5:6] + vala + valb[-1:])
    elif (vala, valb) == (['9,708,996.30'],
                          ['1205', '-', 'LISDEXAMFETAMINE', '-', 'Total',
                           '312,825,210', '3,103.65']):
        return (['1205 - LISDEXAMFETAMINE - Total']
                + valb[5:6] + vala + valb[-1:])
    elif (vala, valb) == (['567,865.82'],
                          ['9652', '-', 'OXYMORPHONE', '-', 'Total',
                           '312,825,210', '181.53']):
        return (['9652 - OXYMORPHONE - Total']
                + valb[5:6] + vala + valb[-1:])


def ltcd_hand_fix(coord0, coord1, coord2, coord3, lt_small_text):
    if ((coord0, coord1, coord2, coord3) == (257, 576, 535, 590) and
       lt_small_text == ('DISTRIBUTION BY STATE IN GRAMS '
                         'PER 100,000 POPULATION')):
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
        val = ['2100', 'BARBITURIC ACID DERIVIATIVE OR SALT [PER  21CFR'
                       ' 1308.13(C)(3)]']
        skip_next_row = 1
        print('need to skip next row')
    if val == ['7365', 'DRONABINOL IN AN ORAL SOLUTION IN FDA APPROVED']:
        val = ['7365', 'DRONABINOL IN AN ORAL SOLUTION IN FDA APPROVED DRUG '
                       'PRODUCT (SYNDROS - CII)']
        skip_next_row = 1
        print('need to skip next row')
    return val, skip_next_row


# def fix_2018_2019_totals(header_dict, row):
#     """
#     The 2018 and 2019 reports for Report 2 have the total line include
#     the drug name. This just replaces that with "total" because
#     otherwise difficult to detect
#     """
#     def list_is_allfloats(li):
#         try:
#             [float(x.replace(',', '')) for x in li]
#             return True
#         except ValueError:
#             return False
#
#     row2 = row
#     if (
#         header_dict['REPORT_PD']
#         and header_dict['REPORT']
#         and header_dict['DRUG_CODE']
#         and header_dict['DRUG_NAME']
#        ):
#         row = [x for x in row if x != '']
#         row = fix_2018_2019_totals_hand_helper(row)
#         if (
#             (header_dict['REPORT_PD'] == ['01/01/2019 TO 12/31/2019']
#              or header_dict['REPORT_PD'] == ['01/01/2018 TO 12/31/2018'])
#             and set(header_dict['REPORT']).issubset(['2', '3', '4'])
#             and len(row) >= 2
#             and list_is_allfloats(row[1:])
#            ):
#             totalregex_2018_2019 = (header_dict['DRUG_CODE'][0]
#                                     + " - " + header_dict['DRUG_NAME'][0]
#                                     + " - Total")
#             if (
#                 (len(row[0]) >= 20 and row[0] == totalregex_2018_2019[:len(row[0])]
#                  )
#                 or (row[0] == totalregex_2018_2019)
#                ):
#                 row2 = ['TOTAL'] + row[1:]
#     return row2
#
#
# def fix_2018_2019_totals_hand_helper(row):
#     return (row[0:1] +
#             list(itertools.chain.from_iterable(
#                 [x.split(' ') for x in row[1:]])))

# def fix_2018_2019_R2_totals_hand_helper(row):
#     if row == ['1100 - AMPHETAMINE - Total',
#                '312,825,210 23,002,150.68', '7,353.04']:
#         return ['1100 - AMPHETAMINE - Total',
#                 '312,825,210', '23,002,150.68', '7,353.04']
#     if row == ['1100 - AMPHETAMINE - Total',
#                '312,825,210 21,888,402.46', '6,997.01']:
#         return ['1100 - AMPHETAMINE - Total',
#                 '312,825,210', '21,888,402.46', '6,997.01']
#     if row == ['9050 - CODEINE - Total',
#                '312,825,210 12,105,984.78', '3,869.89']:
#         return ['9050 - CODEINE - Total',
#                 '312,825,210', '12,105,984.78', '3,869.89']
#     if row == ['1724 - METHYLPHENIDATE (DL;D;L;ISOMERS) - Total',
#                '312,825,210 17,650,670.52', '5,642.34']:
#         return ['1724 - METHYLPHENIDATE (DL;D;L;ISOMERS) - Total',
#                 '312,825,210', '17,650,670.52', '5,642.34']
#     return row


# def pre_categorize_hand_fix(val):
#     if val == ['55,629.20', '55,915.60', '57,420.83',
#                '58,251.72', '227,217.35']:
#         val = ['TOTAL', '55,629.20', '55,915.60', '57,420.83',
#                '58,251.72', '227,217.35']
#     if val == ['DELAWARE']:
#         val = ['STATE:', 'DELAWARE']
#     if val == ['DISTRICT OF COLUMBIA']:
#         val = ['STATE:', 'DISTRICT OF COLUMBIA']
#     print('YES PRE CATEGORIZE')
#     return val
