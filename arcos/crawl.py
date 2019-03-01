import os
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urldefrag


crawl_ext_blacklist = ['.pdf']


def url_link_tree(url, depth=2):
    urllist = [url]
    for n in range(depth):
        urllist = list(set(urllist + find_all_child_links(urllist)))
    return urllist


def find_all_child_links(urllist):
    child_links = []
    for url in urllist:
        new_child_links = find_child_links(url)
        if new_child_links:
            child_links = child_links + new_child_links
    return list(set(child_links))


def find_child_links(baseurl):
    page_links = return_page_links(baseurl)
    child_links = [x for x in page_links if is_child(x, baseurl)]
    return child_links


def return_page_links(baseurl):
    urllist = []
    if is_valid_ext(baseurl):
        r = requests.get(baseurl)
        soup = BeautifulSoup(r.text, 'html.parser')
        for link in soup.find_all('a'):
            h = urldefrag(urljoin(baseurl, link.get('href')))[0]
            if h not in urllist and h != baseurl:
                urllist.append(h)
    return urllist


def is_child(url, baseurl):
    return url.startswith(os.path.dirname(baseurl))


def is_valid_ext(url):
    ext = os.path.splitext(url)[1]
    return ext not in crawl_ext_blacklist
