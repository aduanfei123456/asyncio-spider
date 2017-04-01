import re
import operator
import functools
import urllib.parse

#?

def get_url_legal(url,base_url,encoding=None):
    url_join=urllib.parse.urljoin(base_url,url)
    url_legal=urllib.parse.quote(url_join,safe="%/:=&?~#+!$,;'@()*[]|", encoding=encoding)
    url_frags=urllib.parse.urlparse(url_legal,allow_fragments=True)
    return urllib.parse.urlunparse((url_frags.scheme, url_frags.netloc, url_frags.path, url_frags.params, url_frags.query, ""))

