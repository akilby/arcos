import os
import urllib.request
from urllib.parse import urldefrag, urljoin

import requests
from bs4 import BeautifulSoup

crawl_ext_blacklist = ['.pdf']


def url_link_tree(url, html_only=False, depth=2):
    urllist = [url]
    for n in range(depth):
        urllist = list(set(urllist + find_all_child_links(urllist, html_only)))
    return urllist


def find_all_child_links(urllist, html_only=False):
    child_links = []
    for url in urllist:
        new_child_links = find_child_links(url, html_only)
        if new_child_links:
            child_links = child_links + new_child_links
    return list(set(child_links))


def find_child_links(baseurl, html_only=False):
    page_links = return_page_links(baseurl)
    child_links = [x for x in page_links if is_child(x, baseurl)]
    if html_only:
        child_links = [x for x in child_links if is_html(x)]
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


def content_type(url):
    response = urllib.request.urlopen(url)
    return (dict(response.headers)['Content-Type']
            if 'Content-Type' in dict(response.headers)
            else None)


def is_html(url):
    c = content_type(url)
    if c:
        return True if c.startswith('text/html') else False
    else:
        return False
