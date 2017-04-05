# -*- coding: utf-8 -*-
'''
scrapy不能重复页面

'''
import sys
reload(sys)
sys.setdefaultencoding('utf-8')
import scrapy
from scrapy import Selector,Request
import re
import time
import datetime
import json
import MySQLdb
from mc_acfun_spider.items import McAcfunSpiderItem,PublicItem
import traceback
import os
if "132612612342135" in ''.join(os.uname()):
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

class mc_acfun_spider(scrapy.Spider):
    name='mc_acfun_video_information_spider'
    allowed_domains = ["acfun.cn"]
    gets = ''

    def __init__(self, choice=None):
        super(mc_acfun_spider, self).__init__()
        self.choice = choice
        self.conn = MySQLdb.connect(host=DB_URL, user=DB_USER, passwd=DB_PSW, db=DB_NAME, charset=DB_CHARSET)
        self.cur = self.conn.cursor()

    def start_requests(self):
        if self.choice == 'create':
            #acfun内部系统问题，202页以后的页面都是相同的。
            for i in range(1,21):
                yield Request('http://search.aixifan.com/search?q=%E6%88%91%E7%9A%84%E4%B8%96%E7%95%8C' \
                      '&isArticle=1&cd=1&sys_name=pc&format&pageSize=10' \
                      '&pageNo={}&aiCount=3&spCount=3&type=2&isWeb=1&sortField=views'.format(i),callback=self.get_urls)
                yield Request('http://search.aixifan.com/search?q=%E6%88%91%E7%9A%84%E4%B8%96%E7%95%8C'
                              '&isArticle=1&cd=1&sys_name=pc&format&pageSize=10'
                              '&pageNo={}&aiCount=3&spCount=3&type=2&isWeb=1&sortField=releaseDate'.format(i),callback=self.get_urls)
                yield Request('http://search.aixifan.com/search?q=minecraft'
                              '&isArticle=1&cd=1&sys_name=pc&format&pageSize=10'
                              '&pageNo={}&aiCount=3&spCount=3&type=2&isWeb=1&sortField=views'.format(i),callback=self.get_urls)
                yield Request('http://search.aixifan.com/search?q=minecraft'
                              '&isArticle=1&cd=1&sys_name=pc&format&pageSize=10'
                              '&pageNo={}&aiCount=3&spCount=3&type=2&isWeb=1&sortField=releaseDate'.format(i),callback=self.get_urls)
        elif self.choice=='update':
            d = datetime.datetime.strftime(datetime.date.today() - datetime.timedelta(days=61), '%Y-%m-%d')
            sql='select distinct page_url from src_minecraft_public_video_day where website=%s and play_cnt>1000 and upload_time>%s'
            self.cur.execute(sql,['acfun',d])
            self.gets=self.cur.fetchall()
            for i in list(self.gets):
                acid=re.findall(r'(?<=/ac)\d+',i[0])[0]
                fenp=True if '_' in i[0] else False
                index='' if not re.findall('(?<=_)\d+',i[0]) else int(re.findall('(?<=_)\d+',i[0])[0])
                yield Request(i[0], callback=self.get_video_information,meta={'fenp':fenp,'acid':acid,'index':index})
        else:
            print 'sth wrong.'

    def nowanyi(self,item):
        if '万' in str(item):
            return int(float(item[:-1])*10000)
        elif '亿' in str(item):
            return int(float(item[:-1])*100000000)
        else:
            return item

    def get_urls(self,response):
        d=json.loads(response.body[1:])
        for i in d['data']['page']['list']:
            url='http://www.acfun.cn/v/{}'.format(i['contentId'])
            yield Request(url,callback=self.get_jip,meta={'acid':i['contentId']})

    def get_jip(self,response):
        t=response.xpath('//script')
        try:
            d=json.loads(t[4].xpath('text()').extract()[0][15:])
            if len(d['videoList'])==1:
                yield Request(response.url,callback=self.get_video_information,dont_filter=True,meta={'fenp':False,'acid':response.meta['acid']})
            else:
                for i in range(1,len(d['videoList'])+1):
                    url=response.url+'_{}'
                    yield Request(url.format(i), callback=self.get_video_information, dont_filter=True,meta={'fenp':True,'index':i,'acid':response.meta['acid']})
        except:
            print response.url,'外链视频不抓取，不支持的视频源或视频已经消失。'

    def get_video_information(self,response):
        try:
            item=McAcfunSpiderItem()
            publicItem=PublicItem()
            publicItem['website']='acfun'
            publicItem['crawl_time']=item['crawl_time'] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
            publicItem['page_url']=item['page_url'] = response.url
            t = response.xpath('//script')
            d = json.loads(t[4].xpath('text()').extract()[0][15:])
            publicItem['comment_cnt'] =item['comment_cnt']=self.nowanyi(d['commentCount'])
            item['introduction']=d['description']
            publicItem['title'] =item['title']=d['title']
            item['duration']=str(d['duration'])+'s'
            if response.meta['fenp']:
                item['title_p']=d['videoList'][response.meta['index']-1]['title']
                item['video_src'] = d['videoList'][response.meta['index']-1]['source_type']
                publicItem['video_url'] =item['video_url'] = 'http://cdn.aixifan.com/player/ACFlashPlayer.out.swf?vid={}&ref=http://www.acfun.cn/v/{}_{}'.format(
                    d['videoId'], response.meta['acid'], response.meta['index'])
            else:
                item['title_p']=''
                item['video_src'] = d['videoList'][0]['source_type']
                publicItem['video_url'] =item['video_url'] = 'http://cdn.aixifan.com/player/ACFlashPlayer.out.swf?vid={}&ref=http://www.acfun.cn/v/{}'.format(
                    d['videoId'], response.meta['acid'])
            publicItem['label'] =item['label']=' '.join([i['name'] for i in d['tagList']])
            item['banana_cnt']=self.nowanyi(d['bananaCount'])
            item['collect_cnt']=self.nowanyi(d['favoriteCount'])
            publicItem['category'] =item['category']='主页>'+d['parentChannelName']+'>'+d['channelName']
            publicItem['author'] =item['author']=d['username']
            publicItem['play_cnt'] =item['play_cnt']=self.nowanyi(d['viewCount'])
            item['danmu_cnt']=self.nowanyi(d['danmuSize'])
            publicItem['upload_time'] =item['upload_time']=d['contributeTime'].split()[0]
            publicItem['author_url'] =item['personal_homepage']='http://www.acfun.cn/u/{}.aspx#page=1'.format(d['userId'])
            yield Request(item['personal_homepage'],meta={'item':item,'publicItem':publicItem},callback=self.get_more,dont_filter=True)
        except:
            print traceback.print_exc()
            print response.url, '外链视频不抓取，不支持的视频源或视频已经消失。'

    def get_more(self,response):
        item=response.meta['item']
        publicItem=response.meta['publicItem']
        publicItem['post_cnt'] =item['post_cnt']=self.nowanyi(response.xpath('//span[@class="fl sub"]/text()').extract()[0])
        publicItem['fans_cnt'] =item['listener_cnt'] = self.nowanyi(response.xpath('//span[@class="fl fans"]/text()').extract()[0])
        item['personal_signiture'] = self.nowanyi(response.xpath('//div[@class="infoM"]/text()').extract()[0])
        publicItem['all_play_cnt'] =''
        yield item
        yield publicItem
        #抓前20页意思意思
        for i in range(1,11):
            url=response.url+'#page={}'.format(i)
            yield Request(url,callback=self.get_author_video)


    def get_author_video(self,response):
        for a in response.xpath('//div[@class="contentView"]/a'):
            url='http://www.acfun.cn'+a.xpath('.//figure/@data-url').extract()[0]
            yield Request(url,callback=self.get_jip)







