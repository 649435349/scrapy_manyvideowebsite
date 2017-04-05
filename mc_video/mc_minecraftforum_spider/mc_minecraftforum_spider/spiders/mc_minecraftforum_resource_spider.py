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
from mc_minecraftforum_spider.items import McMinecraftforumSpiderItem
import traceback
import os
import MySQLdb

class mc_minecraftforum_spider(scrapy.Spider):
    name = 'mc_minecraftforum_resource_spider'

    def start_requests(self):
        urls=[('http://www.minecraftforum.net/forums/mapping-and-modding/minecraft-mods?page={}','Mods'),
               ('http://www.minecraftforum.net/forums/mapping-and-modding/maps?page={}','Maps'),
              ('http://www.minecraftforum.net/forums/mapping-and-modding/resource-packs?page={}','Resource Packs'),
               ('http://www.minecraftforum.net/forums/mapping-and-modding/skins?page={}','Skins'),
                ('http://www.minecraftforum.net/forums/mapping-and-modding/minecraft-tools?page={}','Tools')]
        for i in urls:
            for j in range(1,21):
                url=i[0]+'&sort=-datecreated'
                yield Request(url.format(j),callback=self.dododo,meta={'category':i[1]},dont_filter=True)
                url=i[0]+'&sort=-viewcount'
                yield Request(url.format(j), callback=self.dododo, meta={'category': i[1]},dont_filter=True)

    def dododo(self,response):
        for tr in response.xpath('//table[@id="forum-threads"]/tbody/tr'):
            item=McMinecraftforumSpiderItem()
            item['crawl_time'] = time.strftime(
                '%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
            item['website'] = 'minecraftforum'
            item['category'] = response.meta['category']
            item['name']=''
            item['version']='|'.join(tr.xpath('.//span[@class="pc-version"]/text()').extract())
            item['page_url']='http://www.minecraftforum.net'+tr.xpath('.//a[@class="title "]/@href').extract()[0]
            item['title']=tr.xpath('.//a[@class="title "]/text()').extract()[0]
            item['upload_time']=time.strftime('%Y-%m-%d',time.localtime(int(tr.xpath('.//time[@itemprop="datePublished"]/abbr/@data-epoch').extract()[0])))
            item['download_cnt']=''
            item['comment_cnt']=tr.xpath('.//td[@class="col-count"]')[0].xpath('.//a/text()').extract()[0].replace(',','')
            item['author']=tr.xpath('.//div[@class="thread-author"]/a[1]/span[1]/text()').extract()[0]
            item['view_cnt'] = tr.xpath('.//td[@class="col-count"]')[1].xpath('.//@data-count').extract()[0].replace(',',
                                                                                                                  '')
            item['more']=''
            yield item