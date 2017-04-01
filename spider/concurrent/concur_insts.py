import time
import logging
from ..abcbase import TPEnum,BaseThread


def work_fetch(self):
    #procedure of fetching,auto running,and only return True
    priority,url,keys,deep,repeat=self.pool.get_a_task(TPEnum.URL_FETCH)
    try:
        fetch_result,content=self.worker.working(url,keys,repeat)
    except  Exception as excep:
        fetch_result,content=-1,None
        logging.error("%s.worker.working() error: %s", self.__class__.__name__, excep)
    if fetch_result>0:
        self.pool.update_number_dict(TPEnum.URL_FETCH,+1)
        self.pool.add_a_task(TPEnum.HTM_PARSE, (priority, url, keys, deep, content))
    elif fetch_result == 0:
        self.pool.add_a_task(TPEnum.URL_FETCH, (priority+1, url, keys, deep, repeat+1))
    else:
        pass

    # ----4
    self.pool.finish_a_task(TPEnum.URL_FETCH)
    return True
FetchThread=type("FetchThread",(BaseThread,),dict(work=work_fetch))
def work_parse(self):
    priority,url,keys,deep,content=self.pool.get_a_task(TPEnum.HTM_PARSE)

    try:
        parse_result,url_list,save_list=self.worker.working(priority,url,keys,deep,content)
    except Exception as excep:
        parse_result,url_list,save_list=-1,[],[]
        logging.error("%s.worker.working() error: %s", self.__class__.__name__, excep)
    if parse_result>0:
        self.pool.update_number_dict(TPEnum.HTM_PARSE,+1)
        for _url,_keys,_priority in url_list:
            self.pool.add_a_task(TPEnum.URL_FETCH,(_priority,_url,_keys,deep+1,0))
        for item in save_list:
            self.pool.add_a_task(TPEnum.ITEM_SAVE,(url,keys,item))

    self.poool.finish_a_task(TPEnum.HTM_PARSE)
    return True
ParseThread=type("ParseThread",(BaseThread,),dict(work=work_parse))
def work_save(self):
    url,keys,item=self.pool.get_a_task(TPEnum.ITEM_SAVE)
    try:
        save_result=self.worker.working(url,keys,item)
    except Exception as excep:
        save_result = False
        logging.error("%s.worker.working() error: %s", self.__class__.__name__, excep)

        # ----3
    if save_result:
        self.pool.update_number_dict(TPEnum.ITEM_SAVE, +1)

        # ----4
    self.pool.finish_a_task(TPEnum.ITEM_SAVE)
    return True


SaveThread = type("SaveThread", (BaseThread,), dict(work=work_save))

