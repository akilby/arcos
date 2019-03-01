import os
import csv
import itertools
import requests


def main():
    pass
    # if args.subcommand == 'download':
    #     ensure_source_files(args.folder,
    #                         args.base_url,
    #                         args.depth,
    #                         args.url_list_path)


# MY_FOO_FILE = os.path.join(os.path.dirname(__file__), 'foo/bar/file.txt')


def ensure_source_files(folder, base_url, depth, url_list_path):
    if url_list_path:
        urllist = flat_list_from_csv(url_list_path)
        print('%s URLs for download retrieved from %s'
              % (len(urllist), url_list_path))
    else:
        urllist = pdf_links_from_web_tree(base_url, depth=depth)
        print('%s URLs for download retrieved from %s'
              % (len(urllist), base_url))
    broken_links, already_downloaded = download_urllist(urllist, folder)


def pdf_links_from_web_tree(url, depth=2):
    return [x for x in url_link_tree(url, depth=depth) if is_pdf(x)]


def flat_list_from_csv(csv_filepath):
    with open(csv_filepath, 'r', newline='') as f:
        reader = csv.reader(f)
        csvlist = list(reader)
    return list(itertools.chain.from_iterable(csvlist))


def ensure_folder(folder):
    if os.path.isdir(folder) is False:
        os.mkdir(folder)


def download(list_of_urls, folder, overwrite=False):
    ''' Downloads every link from a list of urls. If a file with the
    same basename already exists in the folder, it is skipped.'''

    broken_links, already_downloaded = [], []
    ensure_folder(folder)
    for url in sorted(list_of_urls):
        base = os.path.basename(url)
        save_path = os.path.join(folder, base)
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


def configure():
    pass


def build(folder):
    pass


def is_pdf(url):
    return url.endswith('.pdf')


def download_folder(args):
    print("download: %s" % args.folder)


def build(args):
    print("build: %s" % 'success')


if __name__ == '__main__':
    main()
