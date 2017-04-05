# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html


import sys
reload(sys)
sys.setdefaultencoding('utf-8')
import MySQLdb
import os
op_sys = os.uname()[0]

DB_URL = "10.63.76.38"
DB_USER = "us_player_base"
DB_PSW = "7DY87EEmVXz8qYf2"
DB_NAME = "us_player_base_test"
DB_CHARSET = "utf8"

class McCurseSpiderPipeline(object):
    def __init__(self):
        self.conn=MySQLdb.connect(host=DB_URL,user=DB_USER,passwd=DB_PSW,db=DB_NAME,charset=DB_CHARSET)
        self.cur=self.conn.cursor()

    def process_item(self, item, spider):
        try:
            sql = 'insert into src_minecraft_public_resource' \
                  '(crawl_time,website,category,name,version,page_url,title,upload_time,download_cnt,comment_cnt,author,view_cnt,more)' \
                  ' values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
            self.cur.execute(sql, (item['crawl_time'],item['website'],item['category'],item['name'],item['version'],
                                   item['page_url'],item['title'],item['upload_time'],item['download_cnt'],
                                   item['comment_cnt'],item['author'],item['view_cnt'],item['more']))
            self.conn.commit()
            return item
        except Exception,e:
            print e

    def close_spider(self,*args,**kw):
        self.conn.close()
