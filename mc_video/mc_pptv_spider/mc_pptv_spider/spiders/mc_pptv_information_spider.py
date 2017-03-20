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
from mc_pptv_spider.items import McPptvSpiderItem
import traceback


class mc_pptv_spider(scrapy.Spider):
    name = 'mc_pptv_video_information_spider'
    allowed_domains = ["pptv.com"]

    def start_requests(self):
        #都只抓前100页，如果有的话
        # 先抓上面的
        urls = ['http://search.pptv.com/result?search_query=minecraft&result_type=7&page={}',
                'http://search.pptv.com/result?search_query=minecraft&result_type=-1&page={}',
                'http://search.pptv.com/result?search_query=%E6%88%91%E7%9A%84%E4%B8%96%E7%95%8C+minecraft&result_type=7&page={}',
                'http://search.pptv.com/result?search_query=%E6%88%91%E7%9A%84%E4%B8%96%E7%95%8C+minecraft&result_type=7&page={}']
        for i in urls:
            for j in range(1, 101):
                yield Request(i.format(j), callback=self.get_each_video1)
        #再抓下面的
        urls=['http://search.pptv.com/result?search_query=%E6%88%91%E7%9A%84%E4%B8%96%E7%95%8C+minecraft&result_type=-2&search_sort=video_date_uploaded&p={}#sort',
              'http://search.pptv.com/result?search_query=%E6%88%91%E7%9A%84%E4%B8%96%E7%95%8C+minecraft&result_type=-2&search_sort=view_count&p={}#sort'
              'http://search.pptv.com/result?search_query=minecraft&search_sort=video_date_uploaded&p={}#sort',
              'http://search.pptv.com/result?search_query=minecraft&search_sort=view_count&p={}#sort']
        for i in urls:
            for j in range(1,101):
                yield Request(i.format(j), callback=self.get_each_video2)

    def get_each_video1(self,response):
        try:
            for i in response.xpath('//div[@class="scon cf"]'):
                try:#有很多视频
                    for li in i.xpath('.//ul[@class="dlist2 cf"]/li'):
                        url=li.xpath('.//a[1]/@href').extract()[0]
                        if 'page' not in url:
                            yield Request(url, callback=self.get_video_information)
                except:
                    url = i.xpath('.//div[@class="bd fr"]/a/@href').extract()[0]
                    yield Request(url, callback=self.get_video_information)

        except:
            print traceback.print_exc()

    def get_each_video2(self,response):
        try:
            for i in response.xpath('//div[@class="ui-resp-pics ui-160x90 cf"]/ul[1]/li'):
                url=i.xpath('.//a[1]/@href').extract()[0]
                yield Request(url,callback=self.get_video_information)
        except:
            print traceback.print_exc()

    def get_video_information(self,response):
        item=McPptvSpiderItem()
        item['crawl_time'] = time.strftime(
            '%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
        item['page_url'] = response.url
        item['title']=response.xpath('//meta[@name="title"]/@content').extract()[0]
        item['introduction']=response.xpath('//meta[@name="description"]/@content').extract()[0]
        item['tag']=' '.join(response.xpath('//li[@class="tabs"]/a/@title').extract())
        item['category']=' '.join(response.xpath('//p[@class="crumbs"]/a/@title').extract())
        vid=re.findall(r'(?<=show/)[^.]+(?=.html)',response.url)[0]
        item['video_url']='http://player.pptv.com/v/{}.swf'.format(vid)
        item['upload_time']=''
        item['comment_cnt']=''
        item['author']=''
        pid=re.findall(r'(?<=pid":)\d+',response.xpath('//script/text()').extract()[1])[0]
        url='http://apis.web.pptv.com/show/videoList?from=web&version=1.0.0&format=jsonp&pid={}&cat_id=7&vt=22'.format(pid)
        if pid!='0':
            yield Request(url,callback=self.get_json1,meta={'item':item,'vid':vid},dont_filter=True)

    def get_json1(self,response):
        item=response.meta['item']
        d=json.loads(response.body)
        for i in d['data']['list']:
            if response.meta['vid'] in i['url']:
                item['play_cnt']=self.nowanyi(i['pv'])
                yield item

    def nowanyi(self, item):
        if '万' in str(item):
            return int(float(item[:-1]) * 10000)
        elif '亿' in str(item):
            return int(float(item[:-1]) * 100000000)
        else:
            return item





