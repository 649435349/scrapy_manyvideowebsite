# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html


import MySQLdb
import os
from mc_iqiyi_spider.items import McIqiyiSpiderItem,PublicItem
op_sys = os.uname()[0]

if "1984891651984" in ''.join(os.uname()):
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

class McIqiyiSpiderPipeline(object):
    def __init__(self):
        self.conn=MySQLdb.connect(host=DB_URL,user=DB_USER,passwd=DB_PSW,db=DB_NAME,charset=DB_CHARSET)
        self.cur=self.conn.cursor()

    def process_item(self, item, spider):
        try:
            if isinstance(item,McIqiyiSpiderItem):
                sql = 'insert into src_minecraft_iqiyi_video_day(crawl_time,video_url,page_url,title,category,upload_time,play_cnt,comment_cnt,author,duration,topic,tag,introduction,' \
                      'up_cnt,vote_cnt,personal_homepage,personal_signiture,reward_cnt) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
                self.cur.execute(sql, (item['crawl_time'],item['video_url'],item['page_url'],item['title'],
                                       item['category'],item['upload_time'],item['play_cnt'],item['comment_cnt'],
                                       item['author'],item['duration'],item['topic'],item['tag'],item['introduction'],
                                       item['up_cnt'],item['vote_cnt'],item['personal_homepage'],item['personal_signiture'],
                                       item['reward_cnt']))
                self.conn.commit()
                return item
            else:
                sql = 'insert into src_minecraft_public_video_day' \
                      '(crawl_time,website,video_url,page_url,title,category,upload_time,play_cnt,comment_cnt,label,author,fans_cnt,post_cnt,all_play_cnt,author_url)' \
                      ' values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
                self.cur.execute(sql,
                                 (item['crawl_time'], item['website'], item['video_url'], item['page_url'],
                                  item['title'],
                                  item['category'], item['upload_time'], item['play_cnt'], item['comment_cnt'],
                                  item['label'], item['author'], item['fans_cnt'], item['post_cnt'],
                                  item['all_play_cnt'],item['author_url']))
                self.conn.commit()
                return item
        except Exception,e:
            print e

    def close_spider(self,*args,**kw):
        self.conn.close()
