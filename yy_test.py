import requests
from bs4 import BeautifulSoup
base_url="http://www.yy.com"
rank_url="http://www.yy.com/index/t/rank"
import re
headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.87 Safari/537.36",
        "Host": "www.yy.com",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Encoding": "gzip, deflate, sdch, br",
        "Accept-Language": "zh-CN, zh; q=0.8, en; q=0.6",
        "Cache-Control": "max-age=0",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",

    }
contnt=requests.get(rank_url,headers=headers)
content=BeautifulSoup(contnt.text,"lxml")

#print(content)
rank_l=content.find_all("div",class_="host-rank-list")
total=[]
reg=re.compile(r"\n||\t")
for rank in rank_l:
    host=[]
    li_list=rank.find_all("li")
    for li in li_list:
        a=li.find("a")
        num= a.find("span",class_="num").get_text() if a.find("span",class_="num")  else None
        host.append([a.find("span",class_="name").get_text().strip(),num,a["href"]])
    total.append(host)
#print(total)
for hosts in total:
    for host in hosts:
        url=host[2]
        detail=BeautifulSoup(requests.get(base_url+url,headers=headers).text,"lxml")
        channel=reg.sub("",detail.find("h1",class_="video-name",).get_text(),15) if detail.find("h1",class_="video-name") else None
        host.append(channel)
print(total)