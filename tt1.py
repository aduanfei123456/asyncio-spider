import pymysql
from pybloom_live.pybloom import BloomFilter
conn=pymysql.connect(host="localhost",user="root",password="",db="dangdang_book",charset="utf8")
cursor=conn.cursor()
cursor.execute("select m_url from douban_movies")
bfilter=BloomFilter(1000000,0.001)

urls=cursor.fetchall()
print((urls))
for url in urls:
    bfilter.add(url)
for url in urls[:20]:
    print(url in bfilter)

