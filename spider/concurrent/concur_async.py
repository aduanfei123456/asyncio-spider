import re,sys,time,aiohttp,asyncio,logging,datetime
from ..abcbase import TPEnum,BasePool
from ..utilities import get_url_legal,make_random_useragent

class BaseAsyncPool(BasePool):
    def __init__(self,max_repeat=3,sleep_time=0,max_deep=0,save_pipe=sys.stdout,url_filter=None,loop=None):
        BasePool.__init__(self,url_filter=url_filter)
        self.max_repeat = max_repeat  # default: 3, maximum repeat fetching time for a url
        self.sleep_time = sleep_time  # default: 0, sleeping time after a fetching for a url
        self.max_deep = max_deep  # default: 0, if -1, spider will not stop until all urls are fetched
        self.save_pip = save_pipe  # default: sys.stdout, also can be a file handler
        self.loop=loop or asyncio.get_event_loop()
        self.queue=asyncio.PriorityQueue(loop=self.loop)
        self.start_time=None #start time of this pool
        return
    def start_work_and_wait_done(self, fetcher_num=10, is_over=True):
        #start this pool and wait for finishing
        #fetcher_num:the count of tasks
        try:
            self.start_time=time.time()
            self.loop.run_until_complete(self._start(fetcher_num=fetcher_num))
        except KeyboardInterrupt as excep:
            logging.warning("%s KeyboardInterrupt: %s", self.__class__.__name__, excep)
        finally:
            self.loop.stop()
            self.loop.run_forever()
            self.loop.close()
        return

    async def _start(self,fetcher_num):
        #satrt tasks and wait for finishing
        tasks_list=[asyncio.Task(self.work(index),loop=self.loop)for index in range(fetcher_num)]
        await self.queue.join()
        for task in tasks_list:
            task.cancel()
        self.print_status()
        return
    async def work(self,index):
        #rewrite in subclass
        raise NotImplementedError
    def update_number_dict(self, key, value):
        self.number_dict[key]+=value
        return
    def add_a_task(self, task_name, task_content):
        if(task_content[-1]>0)or(not self.url_filter)or self.url_filter.check_and_add(task_content[1]):
            self.queue.put_nowait(task_content)
            self.update_number_dict(TPEnum.URL_NOT_FETCH,+1)
        return

    async def get_a_task(self,task_name):
        #gwt a task based on task_name
        #?

        task_content=await self.queue.get()
        self.update_number_dict(TPEnum.URL_NOT_FETCH, -1)
        self.update_number_dict(TPEnum.TASKS_RUNNING, +1)
        return task_content
    def finish_a_task(self, task_name):
        self.queue.task_done()
        self.update_number_dict(TPEnum.TASKS_RUNNING,-1)
        return
    def print_status(self):
        #print information of this pool
        info = "%s status: running_tasks=%s;" % (self.__class__.__name__, self.number_dict[TPEnum.TASKS_RUNNING])
        info = "%s status: running_tasks=%s;" % (self.__class__.__name__, self.number_dict[TPEnum.TASKS_RUNNING])

        info += " fetch=(%d, %d);" % (self.number_dict[TPEnum.URL_NOT_FETCH], self.number_dict[TPEnum.URL_FETCH])
        info += " parse=(%d, %d);" % (self.number_dict[TPEnum.HTM_NOT_PARSE], self.number_dict[TPEnum.HTM_PARSE])
        info += " save=(%d, %d);" % (self.number_dict[TPEnum.ITEM_NOT_SAVE], self.number_dict[TPEnum.ITEM_SAVE])

        info += " total_seconds=%d" % (time.time() - self.start_time)
        logging.warning(info)
        return
class AsyncPool(BaseAsyncPool):
    async def work(self,index):
        logging.warning("Worker[%s] start", index)
        headers={"User-Agent":make_random_useragent(),"Accept-Encoding":"gzip"}

        session=aiohttp.ClientSession(loop=self.loop,headers=headers)
        #for i in range(1):
        while True:
                try:
                #get a task
                #priority,url,keys,deep,repeat= await  self.get_a_task(task_name=TPEnum.URL.FETCH)
                    priority, url, keys, deep, repeat = await self.get_a_task(task_name=TPEnum.URL_FETCH)
                #fetch the content of a url
                except asyncio.CancelledError:
                    break

                fetch_result,content=await self.fetch(session,url,keys,repeat,index)
                if fetch_result==1:
                    self.update_number_dict(TPEnum.URL_FETCH,+1)
                #parse the content
                    self.update_number_dict(TPEnum.HTM_NOT_PARSE,+1)
                    parse_result,url_list,save_list=await self.parse(priority,url,keys,deep,content)
                    self.update_number_dict(TPEnum.HTM_NOT_PARSE,-1)
                    if parse_result>0:
                        self.update_number_dict(TPEnum.HTM_PARSE, +1)
                    #add new tasks to queue
                        for _url,_keys,_priority in url_list:
                        #deep +=1
                            self.add_a_task(TPEnum.URL_FETCH,(_priority, _url, _keys, deep+1, 0))
                        for item in save_list:
                            self.update_number_dict(TPEnum.ITEM_NOT_SAVE, +1)
                            save_result = await self.save(url, keys, item)
                            self.update_number_dict(TPEnum.ITEM_NOT_SAVE, -1)

                            if save_result:
                                self.update_number_dict(TPEnum.ITEM_SAVE, +1)
                elif fetch_result==0:
                    self.add_a_task(TPEnum.URL_FETCH,(priority+1,url,keys,deep,repeat+1))
                else:
                    self.update_number_dict(TPEnum.URL_FETCH, +1)#self.finish_a_task()
                    pass
            #finish_a_task not implemented
                self.finish_a_task(task_name=TPEnum.URL_FETCH)
                if self.number_dict[TPEnum.URL_FETCH] % 100 == 0:
                    self.print_status()


        session.close()
        logging.warning("Worker[%s] end",index)
        return

    async def fetch(self,session,url,keys,repeat):
        logging.debug("Fetcher start: keys=%s, repeat=%s, url=%s", keys, repeat, url)
        try:
            response=await session.get(url,params=None,data=None,timeout=5)
            if response.history:
                logging.debug("Fetcher redirect: keys=%s, repeat=%s, url=%s", keys, repeat, url)

            fetch_result, content = 1, (response.status, response.url, await response.text())
            await response.release()
        except Exception as excep:
            if repeat >= self.max_repeat:
                fetch_result, content = -1, None
                logging.error("Fetcher error: %s, keys=%s, repeat=%s, url=%s", excep, keys, repeat, url)
            else:
                fetch_result, content = 0, None
                logging.debug("Fetcher repeat: %s, keys=%s, repeat=%s, url=%s", excep, keys, repeat, url)

        logging.debug("Fetcher end: fetch_result=%s, url=%s", fetch_result, url)
        return fetch_result, content

    async def parse(self, priority, url, keys, deep, content):
        """
        parse the content of a url, must "try, except" and don't change parameters and return
        :return (parse_result, url_list, save_list): parse_result can be -1(parse failed), 1(parse success)
        :return (parse_result, url_list, save_list): url_list is [(url, keys, priority), ...], save_list is [item, ...]
        """
        logging.warning("Parser start: priority=%s, keys=%s, deep=%s, url=%s", priority, keys, deep, url)

        try:
            *_, cur_html = content

            url_list = []
            if (self.max_deep < 0) or (deep < self.max_deep):
                a_list = re.findall(r"<a[\w\W]+?href=\"(?P<url>[\w\W]{5,}?)\"[\w\W]*?>[\w\W]+?</a>", cur_html,
                                    flags=re.IGNORECASE)
                url_list = [(_url, keys, priority + 1) for _url in [get_url_legal(href, url) for href in a_list]]
            else:
                logging.debug("Parser stop parse urls: priority=%s, keys=%s, deep=%s, url=%s", priority, keys, deep,
                              url)

            title = re.search(r"<title>(?P<title>[\w\W]+?)</title>", cur_html, flags=re.IGNORECASE)
            save_list = [(url, title.group("title"), datetime.datetime.now()), ] if title else []

            parse_result = 1
        except Exception as excep:
            parse_result, url_list, save_list = -1, [], []
            logging.error("Parser error: %s, priority=%s, keys=%s, deep=%s, url=%s", excep, priority, keys, deep, url)

        logging.debug("Parser end: parse_result=%s, len(url_list)=%s, len(save_list)=%s, url=%s", parse_result,
                      len(url_list), len(save_list), url)
        return parse_result, url_list, save_list

    async def save(self, url, keys, item):
        """
        save the item of a url, must "try, except" and don't change parameters and return
        :return save_result: True or False
        """
        logging.debug("Saver start: keys=%s, url=%s", keys, url)

        try:
            self.save_pip.write("\t".join([str(i) for i in item]) + "\n")
            self.save_pip.flush()
            save_result = True
        except Exception as excep:
            save_result = False
            logging.error("Saver error: %s, keys=%s, url=%s", excep, keys, url)

        logging.debug("Saver end: save_result=%s, url=%s", save_result, url)
        return save_result



