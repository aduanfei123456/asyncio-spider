import copy
import queue
import logging
import threading
from ..abcbase import TPEnum,BasePool
from .concur_insts import FetchThread,ParseThread,SaveThread

class ThreadPool(BasePool):
    def __init__(self,fetcher,parser,saver,url_filter=None):
        BasePool.__init__(self,url_filter=url_filter)
        self.inst_fetcher=fetcher
        self.inst_parser=parser
        self.inst_save=saver
        self.fetch_queue=queue.PriorityQueue()
        self.parse_queue=queue.PriorityQueue()
        self.save_queue=queue.Queue()
        self.lock=threading.Lock()
        return
    def start_work_and_wait_done(self, fetcher_num=10, is_over=True):
        logging.warning("%s start: fetcher_num=%s, is_over=%s", self.__class__.__name__, fetcher_num, is_over)
        if isinstance(self.inst_fetcher,(list,tuple)):
            fetcher_list=[FetchThread("fetcher-%d"%i,fetcher,self)for i,fetcher in enumerate(self.inst_fetcher)]
        else:
            fetcher_list=[FetchThread("fetcher-%d"%i,copy.deepcopy(self.inst_fetcher),self)for i in range(fetcher_num)]
        threads_list=fetcher_list+[ParseThread("parser",self.inst_parsr,self),SaveThread("saver",self.inst_saver,self)]

        for thread in threads_list:
            thread.setDarmon(True)
            thread.start()
        for thread in threads_list:
            #if thread.is_alive():
                thread.join()

        logging.warning("%s end: fetcher_num=%s, is_over=%s", self.__class__.__name__, fetcher_num, is_over)
        return
    def update_number_dict(self, key, value):
        self.lock.acquire()
        self.number_dict[key]+=value
        self.lock.release()
        return
    def add_a_task(self, task_name, task_content):
        if task_name==TPEnum.URL_FETCH:
            if(task_content[-1]>0) or (not self.url_filter) or self.url_filter.check_and_add(task_content):
                self.fetch_queue.put(task_content,block=True)
                self.update_number_dict(TPEnum.URL_NOT_FETCH,+1)
        elif task_name == TPEnum.HTM_PARSE:
                self.parse_queue.put(task_content, block=True)
                self.update_number_dict(TPEnum.HTM_NOT_PARSE, +1)
        elif task_name == TPEnum.ITEM_SAVE:
                self.save_queue.put(task_content, block=True)
                self.update_number_dict(TPEnum.ITEM_NOT_SAVE, +1)
        else:
            logging.error("%s add_a_task error: parameter task_name[%s] is invalid", self.__class__.__name__,
                              task_name)
            exit()
        return



    def get_a_task(self, task_name):
            task_content=None
            if task_name==TPEnum.URL_FETCH:
                task_content=self.fetch_queue.get(block=True,timeoout=5)
                self.update_number_dict(TPEnum.URL_NOT_FETCH,-1)
            elif task_name == TPEnum.HTM_PARSE:
                logging.warning("get a html task")
                task_content = self.parse_queue.get(block=True, timeout=5)
                self.update_number_dict(TPEnum.HTM_NOT_PARSE, -1)
            elif task_name == TPEnum.ITEM_SAVE:
                task_content = self.save_queue.get(block=True, timeout=5)
                self.update_number_dict(TPEnum.ITEM_NOT_SAVE, -1)
            else:
                logging.error("%s get_a_task error: parameter task_name[%s] is invalid", self.__class__.__name__,
                              task_name)
                exit()
            self.update_number_dict(TPEnum.TASKS_RUNNING,+1)
            return task_content
    def finish_a_task(self, task_name):
            if task_name == TPEnum.URL_FETCH:
                self.fetch_queue.task_done()
            elif task_name == TPEnum.HTM_PARSE:
                self.parse_queue.task_done()
            elif task_name == TPEnum.ITEM_SAVE:
                self.save_queue.task_done()
            else:
                logging.error("%s finish_a_task error: parameter task_name[%s] is invalid", self.__class__.__name__,
                              task_name)
                exit()
            self.update_number_dict(TPEnum.TASKS_RUNNING, -1)
            return