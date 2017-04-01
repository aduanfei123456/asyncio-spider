import asyncio
import pymysql
import aiohttp
from spider.utilities import make_random_useragent
from selenium import webdriver
from spider import concur_async
import logging
import requests
from bs4 import BeautifulSoup
import re
class get_dou(concur_async.AsyncPool):
    def __init__(self):
        self.conn=pymysql.connect(host="localhost",user="root",password="",db="dangdang_book",charset="utf8")
        self.cursor=self.conn.cursor()
        self.conn.autocommit(1)
        super(get_dou,self).__init__()

    async def fetch(self,session,url,keys,repeat):

    #url="http://category.dangdang.com/pg{}-cp01.41.43.05.00.00.html"

        logging.warning("Fetcher start: keys=%s, repeat=%s, url=%s", keys, repeat, url)
        headers = {"User-Agent": make_random_useragent(), "Accept-Encoding": "utf8"}
    #session=aiohttp.ClientSession(loop=loop,headers=headers)
        try:
            await asyncio.sleep(1)
            response=await session.get(url,headers=headers,timeout=5)
            if response.history:

                logging.warning("Fetcher redirect: keys=%s, repeat=%s, url=%s", keys, repeat, url)
            fetch_result,content=1,(response.status,response.url,await response.text())
                # print(response.status)
                #print(await response.text())
            await response.release()
        except Exception as excep:
            if repeat>=self.max_repeat:
                fetch_result,content=-1,None
                logging.error("Fetcher error: %s, keys=%s, repeat=%s, url=%s", excep, keys, repeat, url)
            else:
                fetch_result, content = 0, None
                logging.debug("Fetcher repeat: %s, keys=%s, repeat=%s, url=%s", excep, keys, repeat, url)

        logging.warning("Fetcher end: fetch_result=%s, url=%s", fetch_result, url)
        return fetch_result, content

    async def parse(self,priority,url,keys,deep,content):
        url_list,save_list=[],[]
        logging.debug("Parser start: priority=%s, keys=%s, deep=%s, url=%s"%(priority, keys, deep, url))
        soup=BeautifulSoup(content[2],"lxml")
        if keys[0]=="index":
            div_t=soup.find_all("a",class_="nbg",title=True)
            url_list.extend([(a["href"],("detail",keys[1]),0)for a in div_t])
            next_page=soup.find("span",class_="next")
            if next_page:
                next_page_a=next_page.find("a")
                if next_page_a:
                    url_list.append((next_page_a["href"],("index",keys[1]),1))
        else:
            content=soup.find("div",id="content")
            name_and_year=[item.get_text()for item in content.find("h1").find_all("span")]
            name,year=name_and_year if len(name_and_year)==2 else (name_and_year[0],"")
            movie=[url,name.strip(),year.strip("()")]
            content_left=soup.find("div" ,class_="subject clearfix")
            nbg_soup = content_left.find("a", class_="nbgnbg").find("img")
            movie.append(nbg_soup.get("src") if nbg_soup else "")
            info_tag=content_left.find("div" ,id="info").get_text()
            #intererting
            info_dict=dict([line.strip().split(":",1)  for line in info_tag.split('\n')if line.strip().find(":")>0])
            info = content_left.find("div", id="info").get_text()
            info_dict = dict(
                [line.strip().split(":", 1) for line in info.strip().split("\n") if line.strip().find(":") > 0])

            movie.append(info_dict.get("导演", "").replace("\t", " "))
            movie.append(info_dict.get("编剧", "").replace("\t", " "))
            movie.append(info_dict.get("主演", "").replace("\t", " "))

            movie.append(info_dict.get("类型", "").replace("\t", " "))
            movie.append(info_dict.get("制片国家/地区", "").replace("\t", " "))
            movie.append(info_dict.get("语言", "").replace("\t", " "))

            movie.append(info_dict.get("上映日期", "").replace("\t", " ") if "上映日期" in info_dict else info_dict.get("首播","").replace("\t", " "))
            movie.append(info_dict.get("季数", "").replace("\t", " "))
            movie.append(info_dict.get("集数", "").replace("\t", " "))
            movie.append(info_dict.get("片长", "").replace("\t", " ") if "片长" in info_dict else info_dict.get("单集片长", "").replace("\t", " "))

            movie.append(info_dict.get("又名", "").replace("\t", " "))
            movie.append(info_dict.get("官方网站", "").replace("\t", " "))
            movie.append(info_dict.get("官方小站", "").replace("\t", " "))
            movie.append(info_dict.get("IMDb链接", "").replace("\t", " "))
            content_right=soup.find("div",class_="rating_wrap clearbox")
            if content_right:

                movie.append(content_right.find("strong",class_="ll rating_num").get_text())
                rating_people=content_right.find("a",class_="rating_people")
                movie.append(rating_people.find("span").get_text()if rating_people else "")
                rating_per_list=[ item.get_text() for item in content_right.find_all("span",class_="rating_per")]
                movie.append(",".join(rating_per_list))
            else:
                movie.extend(["","",""])
            assert len(movie)==21,"wrong movie length"
            save_list.append(movie)
        return 1,url_list,save_list
    async def save(self,url,keys,item):
        logging.debug("Saver start: keys=%s, url=%s", keys, url)
        self.cursor.execute("insert into douban_movies (m_url, m_name, m_year, m_imgurl, m_director, m_writer, m_actors, "
                            "m_genre, m_country, m_language, m_release, m_season, m_jishu, m_length, m_alias, m_website, m_dbsite, "
                            "m_imdb, m_score, m_comment, m_starpercent)"
                            " VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);",
                            [i.strip() if i is not None else "" for i in item])



def get_douban_movies():
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.98 Safari/537.36",
        "Host": "movie.douban.com",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Encoding": "gzip, deflate, sdch, br",
        "Accept-Language": "zh-CN, zh; q=0.8, en; q=0.6",
        "Cache-Control": "max-age=0",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "Cookie": "bid=Pd48iLTpsf8"
    }
    all_urls=set()
    resp=requests.get("https://movie.douban.com/tag/", headers=headers, verify=False)
    assert resp.status_code==200,resp.status_code
    soup=BeautifulSoup(resp.text,"lxml")
    a_list=soup.find_all("a",href=re.compile(r"^/tag/",flags=re.IGNORECASE))
    all_urls.update([(a_soup.get_text(),"https://movie.douban.com" +a_soup["href"])for a_soup in a_list])
    logging.warning("all urls:%s",len(all_urls))
    dou_spider=get_dou()
    for tag,url in all_urls:
        dou_spider.set_start_url(url,("index",tag),priority=1)
    dou_spider.start_work_and_wait_done()
    return

get_douban_movies()
















