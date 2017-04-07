import asyncio
import pymysql
import aiohttp
from spider.utilities import make_random_useragent
from selenium import webdriver
from spider import concur_async
import logging
from bs4 import BeautifulSoup
import re
import requests
import random
proxy_l=["36.42.33.13:8080","211.75.72.49:80","120.3.55.174:8998"
         ,"112.25.162.237:8998","183.165.188.31:8998","113.100.48.198:8998",
         "39.65.89.167:8998","120.192.230.34:80","122.224.109.109:80",
         "121.61.101.26:808","210.22.145.46:80"]


def parse( priority, url, keys, deep, content):
    url_list, save_list = [], []
    logging.debug("Parser start: priority=%s, keys=%s, deep=%s, url=%s"%( priority, keys, deep, url))
    soup = BeautifulSoup(content, "lxml")
    if keys[0] == "index":
        div_t = soup.find_all("a", class_="nbgnbg", title=True)
        url_list.extend([(a["href"], ("detail", keys[1], 0)) for a in div_t])
        next_page = soup.find("span", class_="next")
        if next_page:
            next_page_a = next_page.find("a")
            if next_page_a:
                url_list.append((next_page_a["href"], ("index", keys[1], 1)))
    else:
        print(soup)
        content = soup.find("div", id="content")
        name_and_year = [item.get_text() for item in content.find("h1").find_all("span")]
        name, year = name_and_year if len(name_and_year) == 2 else (name_and_year[0], "")
        movie = [url, name.strip(), year.strip("()")]
        content_left = soup.find("div", class_="subject clearfix")
        nbg_soup = content_left.find("a", class_="nbgnbg").find("img")
        movie.append(nbg_soup.get("src") if nbg_soup else "")
        info_tag = content_left.find("div", id="info").get_text()
       # print(info_tag)
        info_dict = dict([line.strip().split(":", 1) for line in info_tag.split('\n') if line.find(":") > 0])
#        info_dict = dict([line.strip().split(":", 1) for line in info_tag.strip().split("\n") if line.strip().find(":") > 0])
        #print(info_dict)
        movie.append(info_dict.get("导演", "").replace("\t", " "))
        movie.append(info_dict.get("编剧", "").replace("\t", " "))
        movie.append(info_dict.get("主演", "").replace("\t", " "))

        movie.append(info_dict.get("类型", "").replace("\t", " "))
        movie.append(info_dict.get("制片国家/地区", "").replace("\t", " "))
        movie.append(info_dict.get("语言", "").replace("\t", " "))

        movie.append(
            info_dict.get("上映日期", "").replace("\t", " ") if "上映日期" in info_dict else info_dict.get("首播", "").replace(
                "\t", " "))
        movie.append(info_dict.get("季数", "").replace("\t", " "))
        movie.append(info_dict.get("集数", "").replace("\t", " "))
        movie.append(
            info_dict.get("片长", "").replace("\t", " ") if "片长" in info_dict else info_dict.get("单集片长", "").replace("\t",
                                                                                                                   " "))

        movie.append(info_dict.get("又名", "").replace("\t", " "))
        movie.append(info_dict.get("官方网站", "").replace("\t", " "))
        movie.append(info_dict.get("官方小站", "").replace("\t", " "))
        movie.append(info_dict.get("IMDb链接", "").replace("\t", " "))
        content_right = soup.find("div", class_="rating_wrap clearbox")
        if content_right:
            movie.append(content_right.find("strong", class_="ll rating_num").get_text())
            rating_people = content_right.find("a", class_="rating_people")
            movie.append(rating_people.find("span").get_text() if rating_people else "")
            rating_per_list = [item.get_text() for item in content_right.find_all("span", class_="rating_per")]
            movie.append(",".join(rating_per_list))
        return movie

        #info_dict = {}
        #"a".split()
        #for tag in info_tag:


loop=asyncio.get_event_loop()
url="https://movie.douban.com/subject/25900945/"

headers = {"User-Agent": make_random_useragent(), "Accept-Encoding": "utf8"}
headers["proxy"]='124.88.67.32:81'
async def getpage():
    async with aiohttp.ClientSession(loop=loop,headers=headers) as session:
        async with session.get(url,headers=headers) as response:

            content=await response.text()
            item=parse(1,"",("detail","love"),0,content)
            conn = pymysql.connect(host="localhost", user="root", password="", db="dangdang_book",charset="utf8")
           # conn = pymysql.connect(host="localhost", user="root", password="", db="dangdang", charset="utf-8")
            cursor = conn.cursor()
            conn.autocommit(1)
            print(item)
            cursor.execute(
                "insert into douban_movies (m_url, m_name, m_year, m_imgurl, m_director, m_writer, m_actors, "
                "m_genre, m_country, m_language, m_release, m_season, m_jishu, m_length, m_alias, m_website, m_dbsite, "
                "m_imdb, m_score, m_comment, m_starpercent)"
                " VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);",
                [i.strip() if i is not None else "" for i in item])
proxie = {
        'http' : 'http://122.193.14.102:80'
    }
#resp = requests.get(url="http://blog.csdn.net/f777x0/article/details/51452838", headers=headers, proxies=proxie)
#print(resp.text)
asyncio.ensure_future(getpage())
loop.run_forever()
