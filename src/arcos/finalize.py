
import pandas as pd
from arcos.data.data import grams_name_dict, opioids_main


def produce_final_data(report_final_dict,
                       geounit,
                       drug_category=None,
                       drugs=None,
                       include_territories=False,
                       outlier_threshold=6,
                       replace_outliers=True,
                       business_activity=False,
                       save_to_file=None):
    assert drug_category in [None, 'MAT', 'major_opioid']
    assert geounit in ['ZIP3', 'STATE', 'U.S.']
    assert not (business_activity and geounit is 'ZIP3')
    if geounit == 'ZIP3' and include_territories:
        print('note: in cleaning routine zip3s are dropped for territories. '
              'could be added back in')

    reports = {reportno: rpt.merge(pd.DataFrame({'DRUG_NAME': opioids_main}))
               for reportno, rpt in report_final_dict.items()
               if reportno != '4'}

    reports = {reportno: rpt for reportno, rpt
               in reports.items()
               if geounit in rpt.columns
               and reportno in grams_name_dict.keys()
               and (not business_activity
                    or (business_activity
                        and 'BUSINESS_ACTIVITY' in rpt.columns))
               and not (include_territories and reportno == '1')}
