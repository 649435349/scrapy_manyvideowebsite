# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html
import MySQLdb
import os
op_sys = os.uname()[0]

if "12351236122222222222222236" in ''.join(os.uname()):
    DB_URL = "127.0.0.1"
    DB_USER = "root"
    DB_PSW = "fyf!!961004"
    DB_NAME = "mc_video"
    DB_CHARSET = "utf8"
else:
    DB_URL = "10.63.76.38"
    DB_USER = "us_player_base"
    DB_PSW = "7DY87EEmVXz8qYf2"
    DB_NAME = "us_player_base_test"
    DB_CHARSET = "utf8"

class McSohuSpiderPipeline(object):
    def __init__(self):
        self.conn=MySQLdb.connect(host=DB_URL,user=DB_USER,passwd=DB_PSW,db=DB_NAME,charset=DB_CHARSET)
        self.cur=self.conn.cursor()

    def process_item(self, item, spider):
        try:
            crawl_time=item['crawl_time']
            video_url=item['video_url']
            page_url=item['page_url']
            title=item['title']
            category=item['category']
            upload_time=item['upload_time']
            play_cnt=item['play_cnt']
            comment_cnt=item['comment_cnt']
            author=item['author']
            involved_cnt=item['involved_cnt']
            tag=item['tag']
            introduction=item['introduction']
            author_label=item['author_label']
            up_cnt = item['up_cnt']
            duration = item['duration']
            sql = 'insert into src_minecraft_sohu_video_day values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
            self.cur.execute(sql, (crawl_time,video_url,page_url,title,category,upload_time,play_cnt,comment_cnt,author,involved_cnt,tag,introduction,author_label,up_cnt,duration))
            self.conn.commit()
            return item
        except Exception,e:
            print e

    def close_spider(self,*args,**kw):
        self.conn.close()
