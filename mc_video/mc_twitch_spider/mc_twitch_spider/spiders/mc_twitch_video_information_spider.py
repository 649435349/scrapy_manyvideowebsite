# -*- coding: utf-8 -*-
'''
scrapy不能重复页面

'''
import sys
reload(sys)
sys.setdefaultencoding('utf-8')
import scrapy
from scrapy import Selector, Request
import re
import time
import datetime
import json
from mc_twitch_spider.items import McTwitchSpiderItem
import traceback
import os
import MySQLdb


DB_URL = "10.63.76.38"
DB_USER = "us_player_base"
DB_PSW = "7DY87EEmVXz8qYf2"
DB_NAME = "us_player_base_test"
DB_CHARSET = "utf8"

class mc_twitch_spider(scrapy.Spider):
    name = 'mc_twitch_video_information_spider'
    gets = ''

    def __init__(self, choice=None):
        super(mc_twitch_spider, self).__init__()
        self.choice = choice
        self.conn = MySQLdb.connect(host=DB_URL, user=DB_USER, passwd=DB_PSW, db=DB_NAME, charset=DB_CHARSET)
        self.cur = self.conn.cursor()

    def start_requests(self):
        if self.choice=='create':
            # 抓前20页意思意思
            urls = [
                'https://api-akamai.twitch.tv/kraken/videos/top?limit=60&offset={}&game=Minecraft&period=&language=&broadcast_type=archive%2Cupload%2Chighlight&sort=views&on_site=1',
                'https://api-akamai.twitch.tv/kraken/videos/top?limit=60&offset={}&game=Minecraft&period=&language=&broadcast_type=archive%2Cupload%2Chighlight&sort=time&on_site=1',
                'https://api-akamai.twitch.tv/kraken/videos/top?limit=60&offset={}&game=Minecraft%3A+Story+Mode&period=week&language=zh&broadcast_type=upload&sort=trending_v3&on_site=1',
                'https://api-akamai.twitch.tv/kraken/videos/top?limit=60&offset={}&game=Minecraft%3A+Story+Mode&period=&language=&broadcast_type=archive%2Cupload%2Chighlight&sort=time&on_site=1', ]
            for i in range(1, 11):
                for j in urls:
                    yield Request(j.format((i-1)*60), callback=self.get_author_name,headers={'client-id':'jzkbprff40iqj646a697cyrvl0zt2m6'})
                    break
                break
        elif self.choice=='update':
            d = datetime.datetime.strftime(datetime.date.today() - datetime.timedelta(days=61), '%Y-%m-%d')
            sql = 'select distinct page_url,author from src_minecraft_public_video_day where website=%s and play_cnt>1000 and upload_time>%s'
            self.cur.execute(sql, ['twitch', d])
            self.gets=self.cur.fetchall()
            for i in self.gets:
                v = re.findall(r'(?<=videos/)\d+', i[0])[0]
                base_url = 'https://api-akamai.twitch.tv/kraken/channels/{}/videos?' \
                           'limit=60&offset=0&broadcast_type=archive%2Cupload%2Chighlight&sort=time&on_site=1'
                yield Request(base_url.format(i[1]), callback=self.get_author_video,meta={'author':i[1],'v':v})
        else:
            print 'sth wrong.'

    def get_author_name(self,response):
        d=json.loads(response.body)
        base_url='https://api-akamai.twitch.tv/kraken/channels/{}/videos?' \
                 'limit=60&offset={}&broadcast_type=archive%2Cupload%2Chighlight&sort=time&on_site=1'
        for i in d['videos']:
            for j in range(1,11):
                yield Request(base_url.format(i['channel']['name'],(j-1) * 60), callback=self.get_author_video,meta={'name':i['channel']['name']},headers={'client-id':'jzkbprff40iqj646a697cyrvl0zt2m6'},dont_filter=True)



    def get_author_video(self,response):
        if self.choice=='create':
            d = json.loads(response.body)
            for i in d['videos']:
                v=re.findall(r'(?<=videos/)\d+',i['url'])[0]
                yield Request('https://api.twitch.tv/kraken/videos/v{}'.format(v),callback=self.get_video_information,meta={'name':response.meta['name'],'post_cnt':d['_total']},headers={'client-id':'jzkbprff40iqj646a697cyrvl0zt2m6'},dont_filter=True)
        elif self.choice=='update':
            d = json.loads(response.body)
            yield Request('https://api.twitch.tv/kraken/videos/v{}'.format(response.meta['v']), callback=self.get_video_information,
                          meta={'name': response.meta['author'], 'post_cnt': d['_total']},
                          headers={'client-id': 'jzkbprff40iqj646a697cyrvl0zt2m6'}, dont_filter=True)

    def get_video_information(self,response):
        d = json.loads(response.body)
        item = McTwitchSpiderItem()
        item['crawl_time'] = time.strftime(
            '%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
        item['website'] = 'twitch'
        item['page_url'] = d['url']
        item['title'] = d['title']
        item['category'] = d['game']
        item['upload_time'] = d['created_at'].split('T')[0]
        item['author'] = d['channel']['name']
        item['post_cnt'] =response.meta['post_cnt']
        item['play_cnt']=d['views']
        item['label'] = item['comment_cnt'] = item['all_play_cnt'] = ''
        item['video_url'] = 'https://player.twitch.tv/?video=v{}&autoplay=false'.format(
            re.findall(r'(?<=videos/)\d+', item['page_url'])[0])
        item['author_url']='https://www.twitch.tv/{}'.format(response.meta['name'])
        url='https://api-akamai.twitch.tv/kraken/channels/{}/follows?offset=0&on_site=1&limit=24&direction=DESC&on_site=1'.format(response.meta['name'])
        yield Request(url,callback=self.get_fans_cnt,meta={'item':item},headers={'client-id':'jzkbprff40iqj646a697cyrvl0zt2m6'},dont_filter=True)

    def get_fans_cnt(self,response):
        d = json.loads(response.body)
        item=response.meta['item']
        item['fans_cnt']=d['_total']
        yield item


