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
import numpy
import json
from mc_planetminecraft_spider.items import McPlanetminecraftSpiderItem
import traceback
import os
import MySQLdb

class mc_mcbbs_spider(scrapy.Spider):
    name = 'mc_planetminecraft_resource_spider'
    allow_domains=['planetminecraft.com']

    def start_requests(self):
        urls = [('http://www.planetminecraft.com/resources/texture_packs/?p={}&sort=order_latest', 'Texture Packs'),
                ('http://www.planetminecraft.com/resources/texture_packs/?p={}&sort=order_popularity', 'Texture Packs'),
                ('http://www.planetminecraft.com/resources/mods/?p={}&order=order_latest', 'Mods'),
                ('http://www.planetminecraft.com/resources/mods/?p={}&order=order_popularity', 'Mods'),
                ('http://www.planetminecraft.com/resources/projects/?p={}&order=order_latest', 'Maps'),
                ('http://www.planetminecraft.com/resources/projects/?p={}&order=order_latest', 'Maps'),
                ('http://www.planetminecraft.com/resources/skins/?p={}&order=order_latest', 'skins'),
                ('http://www.planetminecraft.com/resources/skins/?p={}&order=order_popularity', 'skins')
                ]
        for i in urls:
            for j in range(1,5):
                yield Request(i[0].format(j),callback=self.get_each_tiezi,meta={'category':i[1]})

    def get_each_tiezi(self,response):
        meta=response.meta
        for li in response.xpath('//ul[@class="resource_list"]/li[@class="resource "]'):
            url='http://www.planetminecraft.com'+li.xpath('.//div[@class="r-info"]/a/@href').extract()[0]
            item=McPlanetminecraftSpiderItem()
            item['view_cnt']=li.xpath('.//div[@class="r-details"]/div[2]/span[2]/text()').extract()[0]
            item['download_cnt'] = li.xpath('.//div[@class="r-details"]/div[2]/span[3]/text()').extract()[0]
            item['comment_cnt'] = li.xpath('.//div[@class="r-details"]/div[2]/span[4]/text()').extract()[0]
            item['author']=li.xpath('.//div[@class="contributed"]/a/text()').extract()[0]
            meta['item']=item
            yield Request(url, callback=self.get_detail, meta=response.meta, dont_filter=True)

    def get_detail(self, response):
        item = response.meta['item']
        item['crawl_time'] = time.strftime(
            '%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
        item['website'] = 'planetminecraft'
        item['category'] = response.meta['category']
        item['name']=''
        try:
            item['version']=response.xpath('//table[@class="resource-info"]/tr[2]/td[2]/text()').extract()[0]
        except:
            item['version']=''
        item['page_url']=response.url
        item['title']=response.xpath('//title/text()').extract()[0]
        item['upload_time']=datetime.datetime(*list(numpy.array(map(lambda x : int(x),response.xpath('//div[@class="post-date"]/span[@class="stat"]/text()').extract()[0].split('/')))[[2,0,1]])).strftime('%Y-%d-%m')
        try:
            item['more']=response.xpath('//td[@class="tagged"]/text()').extract()[0].strip('"')
        except:
            item['more']=''
        yield item

