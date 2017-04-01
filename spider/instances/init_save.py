import sys
import logging
from ..utilities import params_chack,return_check

class Saver(object):
    #sys.stdout could be a filehandler
    def __init__(self,save_pipe=sys.stdout):
        self.save_pipe=save_pipe
        return
    @params_chack(object,str,object,object)
    def working(self,url,keys,item):
        logging.debug("%s start: keys=%s, url=%s", self.__class__.__name__, keys, url)
        try:
            save_result=self.item_save(url,keys,item)
        except Exception as excep:
            save_result=False
            logging.error("%s error: %s, keys=%s, url=%s", self.__class__.__name__, excep, keys, url)
        logging.debug("%s end: save_result=%s, url=%s", self.__class__.__name__, save_result, url)
        return save_result
    @return_check(bool)
    def item_save(self,url,keys,item):
        self.save_pipe.write("\t".join([str(i) for i in item]) + "\n")
        self.save_pipe.flush()
        return True
