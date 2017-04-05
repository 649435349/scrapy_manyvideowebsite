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
from mc_ltv_spider.items import McLtvSpiderItem,PublicItem
import traceback


class mc_tudou_spider(scrapy.Spider):
    name = 'mc_ltv_video_information_spider'
    allowed_domains = ["le.com",'letv.com']

    def start_requests(self):
        # 抓取前50页
        urls = [
            'http://search.lekan.letv.com/lekan/apisearch_json.so?from=pc&jf=1&hl=1&dt=1,2&ph=420001,420002&show=4&callback=1&or=3&pn={}&ps=25&wd=%E6%88%91%E7%9A%84%E4%B8%96%E7%95%8C&lc=B84A1ACAE57D476A7722AE6187A663E67A10B72D',
            'http://search.lekan.letv.com/lekan/apisearch_json.so?from=pc&jf=1&hl=1&dt=1,2&ph=420001,420002&show=4&callback=1&or=1&pn={}&ps=25&wd=%E6%88%91%E7%9A%84%E4%B8%96%E7%95%8C&lc=B84A1ACAE57D476A7722AE6187A663E67A10B72D',
            'http://search.lekan.letv.com/lekan/apisearch_json.so?from=pc&jf=1&hl=1&dt=1,2&ph=420001,420002&show=4&callback=1&or=3&pn={}&ps=25&wd=minecraft&lc=B84A1ACAE57D476A7722AE6187A663E67A10B72D',
            'http://search.lekan.letv.com/lekan/apisearch_json.so?from=pc&jf=1&hl=1&dt=1,2&ph=420001,420002&show=4&callback=1&or=1&pn={}&ps=25&wd=minecraft&lc=B84A1ACAE57D476A7722AE6187A663E67A10B72D']
        for i in range(1, 51):
            for j in urls:
                yield Request(j.format(i), callback=self.get_each_video)

    def get_each_video(self,response):
        d=json.loads(response.body[2:-1])
        for i in d['data_list']:
            if i['vid']:
                url='http://www.le.com/ptv/vplay/{}.html'.format(i['vid'])
                yield Request(url,callback=self.get_video_information,meta={'vid':i['vid']})

    def get_video_information(self,response):
        item=McLtvSpiderItem()
        item['crawl_time'] = time.strftime(
            '%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
        item['title']=response.xpath('//title/text()').extract()[0]
        item['page_url'] = response.url
        item['video_url']='http://player.letvcdn.com/lc04_p/201702/16/16/33/16/newplayer/LetvPlayer.swf?vid={}'.format(response.meta['vid'])
        try:
            item['upload_time']=response.xpath('//em[@id="video_time"]/text()').extract()[0]
        except:
            try:
                item['upload_time'] = response.xpath('//b[@class="b_02"]/text()').extract()[0]
            except:
                item['upload_time']='请确认确实没有上传时间。'
        t=response.xpath('//div[@class="crumbs"]')
        item['category']=' '.join(t.xpath('.//a/text()').extract())
        t=response.xpath('//script[1]').extract()[0]
        userId=re.findall(r'(?<=userId:")[^"]+',t)[0]
        if len(re.findall(r'\d+',re.findall(r'(?<=duration:\')[^\']+',t)[0]))==2:
            item['duration']=str(int(re.findall(r'\d+',re.findall(r'(?<=duration:\')[^\']+',t)[0])[0])*60+int(re.findall(r'\d+',re.findall(r'(?<=duration:\')[^\']+',t)[0])[1]))+'s'
        else:
            item['duration'] = str(int(re.findall(r'\d+', re.findall(r'(?<=duration:\')[^\']+', t)[0])[0]) * 3600 + int(
                re.findall(r'\d+', re.findall(r'(?<=duration:\')[^\']+', t)[0])[1])*60+int(
                re.findall(r'\d+', re.findall(r'(?<=duration:\')[^\']+', t)[0])[2])) + 's'
        url='http://v.stat.letv.com/vplay/getIdsInfo?type=vlist&ids={}&callback=1'.format(response.meta['vid'])
        yield Request(url,callback=self.get_json1,meta={'item':item,'userId':userId})

    def get_json1(self,response):
        d = json.loads(response.body[3:-3])
        item=response.meta['item']
        item['play_cnt']=d['play_count']
        item['up_cnt']=str(d['up'])+'/'+str(d['down'])
        item['comment_cnt']=d['vcomm_count']+d['vreply']
        url='http://api.chuang.le.com/outer/ugc/video/user/videocount?callback=1&userid={}'.format(response.meta['userId'])
        yield Request(url,callback=self.get_json2,meta={'item':item})

    def get_json2(self,response):
        d = json.loads(response.body[2:-1])
        item = response.meta['item']
        item['author']=d['data']['nickname']
        yield item
        publicItem = PublicItem()
        publicItem['crawl_time'] = item['crawl_time']
        publicItem['website'] = 'ltv'
        publicItem['video_url'] = item['video_url']
        publicItem['page_url'] = item['page_url']
        publicItem['title'] = item['title']
        publicItem['category'] = item['category']
        publicItem['upload_time'] = item['upload_time']
        publicItem['play_cnt'] = item['play_cnt']
        publicItem['comment_cnt'] = item['comment_cnt']
        publicItem['label'] = ''
        publicItem['author'] = item['author']


