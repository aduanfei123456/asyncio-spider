import time
import random
import logging
import requests
from ..utilities import make_random_useragent,params_chack,return_check

class Fetcher(object):
    def __init__(self,max_repeat=3,sleep_time=0):
        self.max_repeat = max_repeat  # default: 3, maximum repeat fetching time for a url
        self.sleep_time = sleep_time  # default: 0, sleeping time after a fetching for a url
        return
    @return_check(int,object)
    #rewrite refer to working
    def url_fetch(self,url,keys,repeat):
        headers={"User-Agent":make_random_useragent(),"Accept-Encoding":"gzip"}
        response=requests.get(url,params=None,data=None,headers=headers,cookie=None,timeout=(3.05,10))
        if response.history:
            logging.debug("%s redirect: keys=%s, repeat=%s, url=%s", self.__class__.__name__, keys, repeat, url)
        content=(response.status_code,response.url,response.text)
        return 1,content
    @params_chack(object,str,object,int)
    def working(self,url,keys,repeat):
        logging.debug("%s start:keys=%s,repeat=%s,url=s",self.__class__.__name__,keys,repeat,url)
        time.sleep(random.randint(0,self.sleep_time))
        try:
           fetch_result,content= self.url_fetch(url,keys,repeat)
        except Exception as excep:
            if repeat>=self.max_repeat:
                fetch_result,content=-1,None
                logging.error("%s error:%s,key=%s,repeaat=%s,url=%s",self.__class__.__name__,excep,keys,repeat,url)
            else:
                fetch_result,content=0,None
                logging.debug("%s repeat: %s, keys=%s, repeat=%s, url=%s", self.__class__.__name__, excep, keys, repeat ,url)
        logging.debug("%s end: fetch_result=%s, url=%s", self.__class__.__name__, fetch_result, url)
        return fetch_result, content

