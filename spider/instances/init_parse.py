import re
import logging
import datetime
from ..utilities import get_url_legal, params_chack, return_check

class Parser(object):
    def __init__(self,max_deep=0):
        self.max_deep=max_deep
        return
    @params_chack(object,int,str,object,int,object)
    def working(self,priority,url,keys,deep,content):
        #return(parse_result -1/1,url_list[(url,keys,priority),...],savelis[item,...]
        logging.debug("%s start: priority=%s, keys=%s, deep=%s, url=%s", self.__class__.__name__, priority, keys, deep,  url)
        try:
            parse_result,url_list,save_list=self.html_parse(priority,url,keys,deep,content)
        except Exception as excep:
            parse_result, url_list, save_list = -1, [], []
            logging.error("%s error: %s, priority=%s, keys=%s, deep=%s, url=%s", self.__class__.__name__, excep,
                          priority, keys, deep, url)

        logging.debug("%s end: parse_result=%s, len(url_list)=%s, len(save_list)=%s, url=%s", self.__class__.__name__,
                      parse_result, len(url_list), len(save_list), url)
        return parse_result, url_list, save_list

    @return_check(int,(tuple,list),(tuple,list))
    def html_parse(self,priority,url,keys,deep,content):
        #parse the content of a url
        #?


        *_,cur_html=content
        url_lisst=[]
        if(self.max_deep<0) or(deep<self.max_deep):
            #命名匹配组
            a_list=re.findall(r"<a[\w\W]+?href=\"(?P<url>[\w\W]{5,}?)\"[\w\W]*?>[\w\W]+?</a>",cur_html,re.IGNORECASE)
            url_list=[(_url,keys,priority+1)for _url in [get_url_legal(href,url)for href in a_list]]
        else:
            logging.debug("%s stop parse urls: priority=%s, keys=%s, deep=%s, url=%s", self.__class__.__name__, priority, keys, deep, url)

        title=re.search(r"<title>(?P<title>[\w\W]+?)</tilte>",cur_html,flags=re.IGNORECASE)
        save_list=[(url,title.group("title"),datetime.datetime.now()),]if title else []
        return 1,url_list,save_list
