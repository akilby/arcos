import os

import requests

from .files import finalize_download_list, prepare_folder


def download(folder, overwrite=False,
             base_url='https://www.deadiversion.usdoj.gov/'
                      'arcos/retail_drug_summary/index.html',
             use_download_list=False):
    """
    Downloads ARCOS files to folder. If use_download_list is
    specified, it simply downloads that list. If it is not (the
    default), base_url is crawled down to depth 2 for PDF links.
    """
    ignore_list = prepare_folder(folder, overwrite)
    download_list = finalize_download_list(base_url,
                                           use_download_list, ignore_list)
    broken_links = download_to_folder(download_list, folder)
    print('%s links failed to download' % len(broken_links))
    if len(broken_links) > 0:
        print(broken_links)


def download_to_folder(download_list, folder):
    broken_links = []
    for url in sorted(download_list):
        save_path = os.path.join(folder, os.path.basename(url))
        success = save_to_disk(url, save_path)
        if not success:
            broken_links.append(url)
    return broken_links


def save_to_disk(url, save_path):
    """
    Saves to disk non-destructively (xb option will not overwrite)
    """
    print('Downloading: %s' % url)
    r = requests.get(url)
    if r.status_code == 404:
        print('URL broken, unable to download: %s' % url)
        return False
    else:
        with open(save_path, 'xb') as f:
            f.write(r.content)
    return True
