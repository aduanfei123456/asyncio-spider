import enum
import queue
import logging
import threading

class TPEnum(enum.Enum):
    TASKS_RUNNING = "tasks_running"  # flag of tasks_running

    URL_FETCH = "url_fetch"  # flag of url_fetched
    HTM_PARSE = "htm_parse"  # flag of htm_parsed
    ITEM_SAVE = "item_save"  # flag of item_saved

    URL_NOT_FETCH = "url_not_fetch"  # flag of url_not_fetch
    HTM_NOT_PARSE = "htm_not_parse"  # flag of htm_not_parse
    ITEM_NOT_SAVE = "item_not_save"  # flag of item_not_save
class BaseThread(threading.Thread):
    def __init__(self,name,worker,pool):
        threading.Thread.__init__(self,name=name)

        self.worker=worker #the worker of each thread
        self.pool=pool
        return

    def run(self):
        #rewrite run function,must call self.work()
        logging.warning("%s[%s] start", self.__class__.__name__, self.getName())
        while True:
            #try:
            if not self.work():
                logging.warning("not work")
                break
            '''except queue.Empty:
                if self.pool.is_all_tasks_done():
                    break
                pass'''
        logging.warning("%s[%s] end", self.__class__.__name__, self.getName())
        return
    def work(self):
        #procedure of each thread,return true to continue,False to stop
        raise NotImplementedError

class BasePool(object):
    #base pool of each pool
    def __init__(self,url_filter=None):
        self.url_filter=url_filter
        self.number_dict={
            TPEnum.TASKS_RUNNING: 0,  # the count of tasks which are running

            TPEnum.URL_FETCH: 0,  # the count of urls which have been fetched successfully
            TPEnum.HTM_PARSE: 0,  # the count of urls which have been parsed successfully
            TPEnum.ITEM_SAVE: 0,  # the count of urls which have been saved successfully

            TPEnum.URL_NOT_FETCH: 0,  # the count of urls which haven't been fetched
            TPEnum.HTM_NOT_PARSE: 0,  # the count of urls which haven't been parsed
            TPEnum.ITEM_NOT_SAVE: 0,  # the count of urls which haven't been saved

        }
        return
    def set_start_url(self,url,keys=None,priority=0,deep=0):
        #set start url based on "keys", "priority" and "deep", repeat must be 0
        logging.warning("%s set_start_url: keys=%s, priority=%s, deep=%s, url=%s", self.__class__.__name__, keys,
                        priority, deep, url)
        self.add_a_task(TPEnum.URL_FETCH,(priority,url,keys,deep,0))
        return

    def start_work_and_wait_done(self, fetcher_num=10, is_over=True):
        """
        start this pool, and wait for finishing
        """
        raise NotImplementedError

    def is_all_tasks_done(self):
        """
        check if all tasks are done, according to self.number_dict
        """
        return False if self.number_dict[TPEnum.TASKS_RUNNING] or self.number_dict[TPEnum.URL_NOT_FETCH] or \
                        self.number_dict[TPEnum.HTM_NOT_PARSE] or self.number_dict[TPEnum.ITEM_NOT_SAVE] else True

    def update_number_dict(self, key, value):
        """
        update self.number_dict of this pool
        """
        raise NotImplementedError

    def add_a_task(self, task_name, task_content):
        """
        add a task based on task_name, if queue is full, blocking the queue
        """
        raise NotImplementedError

    def get_a_task(self, task_name):
        """
        get a task based on task_name, if queue is empty, raise queue.Empty
        """
        raise NotImplementedError

    def finish_a_task(self, task_name):
        """
        finish a task based on task_name, call queue.task_done()
        """
        raise NotImplementedError