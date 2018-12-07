import os
import csv
import requests
import argparse
import itertools
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urldefrag


parser = argparse.ArgumentParser(description='Program builds DEA ARCOS data')

subparsers = parser.add_subparsers(dest='subroutine',
                                   description='Subcommands required.')

parser_build = subparsers.add_parser('configure')

parser_download = subparsers.add_parser('download')
parser_download.add_argument('--folder', required=True,
                             help='Folder in which ARCOS PDFs will be stored '
                                  '(required)')

group = parser_download.add_mutually_exclusive_group(required=False)
group.add_argument('--base-url',
                   default='https://www.deadiversion.usdoj.gov/arcos/'
                           'retail_drug_summary/index.html',
                   help='Specify here the root url for the website '
                        'where the DEA stores the ARCOS PDFs. By default '
                        'this program will crawl down to a depth of 2 to '
                        'find all PDFs. (default: https://www.deadiversion'
                        '.usdoj.gov/arcos/retail_drug_summary/index.html)')
parser_download.add_argument('--depth', type=int,
                             help='Can specify depth to crawl from the base '
                                  'url. (default: 2)')
group.add_argument('--url-list-path',
                   help="If for some reason crawling the DEA website doesn't "
                        'work, or you want to specify a specific list of URLs '
                        'for the program to download, you can specify the '
                        'path of such a file here, overriding base-url. An '
                        'example file containing a current list of pdf links '
                        'can be found in config/optional_download_config.txt')

parser_build = subparsers.add_parser('build')


args = parser.parse_args()

if args.subroutine == 'download':
    if args.depth and args.url_list_path:
        parser.error('--depth only compatible with --base-url')

    if not args.depth:
        args.depth = 2

# print(args)


def main():
    if args.subroutine == 'download':
        ensure_source_files(args.folder,
                            args.base_url,
                            args.depth,
                            args.url_list_path)


def ensure_source_files(folder, base_url, depth, url_list_path):
    if url_list_path:
        urllist = list_from_csv(url_list_path)
        print('%s URLs for download retrieved from %s'
              % (len(urllist), url_list_path))
    else:
        urllist = pdf_links_from_web_tree(base_url, depth=depth)
        print('%s URLs for download retrieved from %s'
              % (len(urllist), base_url))
    broken_links, already_downloaded = download_files(urllist, folder)


def pdf_links_from_web_tree(url, depth=2):
    return [x for x in url_tree(url, depth=depth) if is_pdf(x)]


def list_from_csv(csv_filepath):
    with open(csv_filepath, 'r', newline='') as f:
        reader = csv.reader(f)
        csvlist = list(reader)
    return list(itertools.chain.from_iterable(csvlist))


def url_tree(url, depth=2):
    urllist = [url]
    for n in range(depth):
        urllist = list(set(urllist + find_all_child_links(urllist)))
    return urllist


def find_child_links(baseurl):
    if not is_pdf(baseurl):
        page_links = return_page_links(baseurl)
        child_links = [x for x in page_links
                       if is_child(x, baseurl)]
        return child_links
    return None


def find_all_child_links(urllist):
    child_links = []
    for url in urllist:
        new_child_links = find_child_links(url)
        if new_child_links:
            child_links = child_links + new_child_links
    return list(set(child_links))


def return_page_links(baseurl):
    r = requests.get(baseurl)
    soup = BeautifulSoup(r.text, 'html.parser')
    urllist = []
    for link in soup.find_all('a'):
        h = urldefrag(urljoin(baseurl, link.get('href')))[0]
        if h not in urllist and h != baseurl:
            urllist.append(h)
    return urllist


def is_child(url, baseurl):
    return url.startswith(os.path.dirname(baseurl))


def is_pdf(url):
    return url.endswith('.pdf')


def download_files(list_of_urls, folder):
    broken_links = []
    already_downloaded = []
    if os.path.isdir(folder) is False:
        os.mkdir(folder)
    for url in sorted(list_of_urls):
        save_path = os.path.join(folder,
                                 url.split('/')[len(url.split('/')) - 1])
        if os.path.isfile(save_path):
            already_downloaded.append(url)
            print('Target file already exists: %s' % save_path)
        else:
            print('Downloading: %s' % url)
            r = requests.get(url)
            if r.status_code == 404:
                broken_links.append(url)
            else:
                with open(save_path, 'wb') as f:
                    f.write(r.content)
    print('Unable to download from %s broken links: %s' %
          (len(broken_links), broken_links))
    return broken_links, already_downloaded


if __name__ == '__main__':
    main()
