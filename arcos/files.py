import os
import shutil
from .crawl import url_link_tree

junk_files = ['.DS_Store']


def prepare_folder(folder, overwrite):
    """
    Handles a variety of cases to prepare write folder.
    Returns list of valid files to keep in folder and not overwrite.
    """
    if overwrite:
        shutil.rmtree(folder)
    if not os.path.isdir(folder):
        os.mkdir(folder)

    files, subfolders, junk = query_folder_contents(folder)
    if files or subfolders:
        user_input = prompt_overwrite_or_complete(folder, files, subfolders)
        if user_input == 'overwrite':
            shutil.rmtree(folder)
            os.mkdir(folder)
            files, subfolders, junk = query_folder_contents(folder)
    return files


def finalize_download_list(base_url, use_download_list, ignore_list):
    """
    Generates the download list, either by crawling the DEA website starting
    at the base url, or using a download list. Also subtracts any files
    in passed ignore list. Ignore list used to not re-download valid files
    already present in folder.
    """
    if use_download_list:
        with open(use_download_list, 'r') as f:
            text = f.read()
        urls = text.splitlines()
    else:
        urls = [x for x in url_link_tree(base_url) if x.endswith('.pdf')]
    return [x for x in urls if os.path.basename(x) not in ignore_list]


def query_folder_contents(folder):
    items = os.listdir(folder)
    subfolders, files, junk = [], [], []
    for x in items:
        if x in junk_files:
            junk.append(x)
        elif os.path.isdir(os.path.join(folder, x)):
            subfolders.append(x)
        else:
            files.append(x)
    return files, subfolders, junk


def prompt_overwrite_or_complete(folder, files, subfolders):
    print('Folder "%s" already exists and contains %s files and %s subfolders:'
          % (folder, len(files), len(subfolders)))
    print('Files:\n', files)
    print('Subfolders:\n', subfolders)
    str_input = ''
    while str_input.lower() not in ['overwrite', 'finish']:
        str_input = input('Overwrite and reconstruct entire folder, deleting '
                          'all files ["overwrite"], or preserve files and fini'
                          'sh downloading if any files remain ["finish"]?\n> ')
    return str_input.lower()
