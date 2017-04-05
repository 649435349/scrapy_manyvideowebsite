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
from mc_sohu_spider.items import McSohuSpiderItem
import traceback


class mc_sohu_spider(scrapy.Spider):
    name = 'mc_sohu_video_information_spider'
    allowed_domains = ["sohu.com"]

    def start_requests(self):
        # 搜狐最新和最热最多有100页。
        urls = ['http://so.tv.sohu.com/mts?wd=%E6%88%91%E7%9A%84%E4%B8%96%E7%95%8C+minecraft&c=0&v=0&length=0&limit=0&o=1&p={}&st=&suged=',
                'http://so.tv.sohu.com/mts?wd=%E6%88%91%E7%9A%84%E4%B8%96%E7%95%8C+minecraft&c=0&v=0&length=0&limit=0&o=3&p={}&st=&suged=',
                'http://so.tv.sohu.com/mts?wd=minecraft&c=0&v=0&length=0&limit=0&o=1&p={}&st=&suged=',
                'http://so.tv.sohu.com/mts?wd=minecraft&c=0&v=0&length=0&limit=0&o=3&p={}&st=&suged=']
        for i in urls:
            for j in range(1, 101):
                yield Request(i.format(j), callback=self.get_each_video)

    def get_each_video(self,response):
        for i in response.xpath('//ul[@class="list170 cfix"]/li'):
            url='http:'+i.xpath('.//strong[@class="lt-title"]/a[1]/@href').extract()[0]
            yield Request(url,callback=self.get_video_information)

    def get_video_information(self,response):
        item=McSohuSpiderItem()
        item['crawl_time'] = time.strftime(
            '%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
        item['page_url']=response.url
        item['video_url']=response.xpath('//meta[@property="og:videosrc"]/@content').extract()[0]
        item['introduction']=response.xpath('//meta[@name="description"]/@content').extract()[0]
        item['tag']=' '.join(response.xpath('//meta[@name="keywords"]/@content').extract()[0].split(',')[:-1])
        item['title']=response.xpath('//meta[@property="og:title"]/@content').extract()[0]
        try:
            item['author_label']=response.xpath('//span[@class="sr h18"]')[1].xpath('text()').extract()[0]
        except:
            item['author_label']=''
        t=response.xpath('//script[5]').extract()[0]
        item['upload_time']=re.findall(r'(?<=uploadTime:\s\')[^\']+',t)[0]
        item['duration'] =re.findall(r'(?<=videoLength:\s\')[^\']+',t)[0]+'s'
        item['author'] = re.findall(r'(?<=username=\')[^\']+', t)[0]
        vid=re.findall(r'(?<=vid\s=\s\')[^\']+', t)[0]
        item['category']=' '.join(response.xpath('//div[@class="crumbs"]/a/text()').extract())
        url='http://vstat.my.tv.sohu.com/dostat.do?method=getVideoPlayCount&v={}&n='.format(vid)
        yield Request(url,callback=self.get_json1,meta={'item':item,'vid':vid})

    def get_json1(self,response):
        item=response.meta['item']
        d=json.loads(response.body[6:-2])
        item['play_cnt']=d['count']
        url='http://score.my.tv.sohu.com/digg/get.do?vid={}&type=9001&callback=1'.format(response.meta['vid'])
        yield Request(url,callback=self.get_json2,meta={'item':item,'vid':response.meta['vid']})

    def get_json2(self,response):
        item = response.meta['item']
        d = json.loads(response.body.replace('\r\n','')[2:-1])
        item['up_cnt']=d['upCount']
        url='http://changyan.sohu.com/api/2/topic/load?client_id=cyqyBluaj&topic_url=&topic_source_id=bk{}&topic_category_id=&page_size=100&hot_size=&topic_owner_id=&topic_parent_id=&callback=1'.format(response.meta['vid'])
        yield Request(url,callback=self.get_json3,meta={'item':item})

    def get_json3(self,response):
        item = response.meta['item']
        d = json.loads(response.body)
        item['comment_cnt']=d['cmt_sum']
        item['involved_cnt']=d['participation_sum']
        yield item

