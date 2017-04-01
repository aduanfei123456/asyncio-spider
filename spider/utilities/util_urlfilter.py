import re
import pybloom_live
from .util_config import CONFIG_URLPATTERN_ALL

#filter url by regexs and (bloomfilter or set)
class UrlFilter(object):
    #constructor,use variable of BloomFilter if capacity else variable of set
    def __init__(self,black_patterns=(CONFIG_URLPATTERN_ALL,),white_patterns=("^http",),capacity=None):
        self.re_black_list=[re.compile(_pattern,flags=re.IGNORECASE) for _pattern in  black_patterns]
        self.re_white_list=[re.compile(_pattern,flags=re.IGNORECASE)for _pattern in white_patterns]
        self.url_set=set() if not capacity else None
        self.bloom_filter=pybloom_live.ScalableBloomFilter(capacity,error_rate=0.001)if capacity else None
        return
    #update the urlfilter using urllist
    def update(self,url_list):
        if self.url_set is not None:
            self.url_set.update(url_list)
        elif self.bloom_filter is not None:
            for url in url_list:
                self.bloom_filter.add(url)
        else:
            pass
        return
    #check the url accordding to black/white patterns
    def check_and_add(self,url):
        for re_black in self.re_black_list:
            if re_black.search(url):
                return False
        result=False
        for re_white in self.re_white_list:
            if re_white.search(url):
                if self.url_set is not None:
                    result=(not(url in self.url_set))
                    self.url_set.add(url)
                elif self.bloom_filter is not None:
                    # "add": if key already exists, return True, else return False
                    result=(not(self.bloom_filter.add(url)))
                else:
                    pass
                break
        return result






