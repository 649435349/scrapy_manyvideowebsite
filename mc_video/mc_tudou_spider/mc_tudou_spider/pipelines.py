# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html
import MySQLdb
import os
from mc_tudou_spider.items import McTudouSpiderItem,PublicItem
op_sys = os.uname()[0]

if "fengyufei" in ''.join(os.uname()):
    DB_URL = "127.0.0.1"
    DB_USER = "root"
    DB_PSW = "fyf!!961004"
    DB_NAME = "mc_video"
    DB_CHARSET = "utf8"

class McTudouSpiderPipeline(object):
    def __init__(self):
        self.conn=MySQLdb.connect(host=DB_URL,user=DB_USER,passwd=DB_PSW,db=DB_NAME,charset=DB_CHARSET)
        self.cur=self.conn.cursor()

    def process_item(self, item, spider):
        try:
            if isinstance(item,McTudouSpiderItem):
                crawl_time=item['crawl_time']
                video_url=item['video_url']
                page_url=item['page_url']
                title=item['title']
                category=item['category']
                upload_time=item['upload_time']
                play_cnt=item['play_cnt']
                comment_cnt=item['comment_cnt']
                author=item['author']
                duration=item['duration']
                personal_homepage = item['personal_homepage']
                tag=item['tag']
                introduction=item['introduction']
                up_cnt=item['up_cnt']
                post_cnt=item['post_cnt']
                fans_cnt=item['fans_cnt']
                follower_cnt=item['follower_cnt']
                sql = 'insert into src_minecraft_tudou_video_day values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
                self.cur.execute(sql, (crawl_time,video_url,page_url,title,category,
                                       upload_time,play_cnt,comment_cnt,author,
                                       duration,personal_homepage,tag,introduction,
                                       up_cnt,post_cnt,fans_cnt,follower_cnt))
                self.conn.commit()
                return item
            else:
                sql = 'insert into src_minecraft_public_video_day(crawl_time,website,video_url,page_url,title,category,upload_time,play_cnt,comment_cnt,label,author,fans_cnt,post_cnt,all_play_cnt)' \
                      ' values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
                self.cur.execute(sql,
                                 (item['crawl_time'], item['website'], item['video_url'], item['page_url'],
                                  item['title'],
                                  item['category'], item['upload_time'], item['play_cnt'], item['comment_cnt'],
                                  item['label'], item['author'], item['fans_cnt'], item['post_cnt'],
                                  item['all_play_cnt']))
                self.conn.commit()
                return item
        except Exception,e:
            print e

    def close_spider(self,*args,**kw):
        self.conn.close()
