# import os
# import csv

# # config_path = '/Users/angelakilby/Dropbox/Research/Packages/arcos/arcos/config/arcos_configure.txt'

# local_dir = os.path.dirname(os.path.abspath(__file__))
# config_path = os.path.join(local_dir, 'config', 'arcos_configure.txt')


# def read_config_file():
#     if check_config(config_path):
#         return dict_from_csv(config_path)['Root Directory:']


# def dict_from_csv(csv_filepath):
#     with open(csv_filepath, 'r', newline='') as f:
#         reader = csv.reader(f, delimiter='\t')
#         csvlist = dict(reader)
#     return csvlist


# def check_config(config_filepath):
#     if os.path.isfile(config_filepath):
#         if 'Root Directory:' in dict_from_csv(config_filepath).keys():
#             return True
#     return False


# master_dir = read_config_file()
# test = input("Master working directory [%s]: " % master_dir)
# print(test)
