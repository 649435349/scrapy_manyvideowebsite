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
from mc_curse_spider.items import McCurseSpiderItem
import traceback
import os
import MySQLdb

class mc_curse_spider(scrapy.Spider):
    name = 'mc_curse_resource_spider'

    def start_requests(self):
        urls=[('https://mods.curse.com/bukkit-plugins/minecraft?page={}','bukkit-plugins'),
              ('https://mods.curse.com/mc-mods/minecraft?page={}','mc-mods'),
              ('https://mods.curse.com/worlds/minecraft?page={}','worlds'),
              ('https://mods.curse.com/texture-packs/minecraft?page={}','texture-packs'),
              ('https://mods.curse.com/modpacks/minecraft?page={}','modpacks'),
              ('https://mods.curse.com/customization/minecraft?page={}','customization'),
              ('https://mods.curse.com/mc-addons/minecraft?page={}','mc-addons')]
        for i in urls:
            for j in range(1,20):
                yield Request(i[0].format(j)+'&filter-project-sort=4',callback=self.get_each,meta={'category':i[1]},dont_filter=True)
                yield Request(i[0].format(j) + '&filter-project-sort=5', callback=self.get_each, meta={'category': i[1]},
                              dont_filter=True)

    def get_each(self,response):
        for li in response.xpath('//div[@id="addons-browse"]').xpath('.//ul[@class="listing listing-project project-listing"]/li'):
            url='https://mods.curse.com'+li.xpath('.//ul/li[@class="title"]/h4/a/@href').extract()[0]
            yield Request(url,callback=self.get_resource_information,meta={'category':response.meta['category']})

    def get_resource_information(self,response):
        item=McCurseSpiderItem()
        item['crawl_time']=time.strftime(
            '%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
        item['website']='curse'
        item['category']=response.meta['category']
        item['name']=''
        try:
            item['version']=re.findall(r'(?<=Supports:\s).+',response.xpath('//li[contains(@class,"version")]/text()').extract()[0])[0]
        except:
            item['version'] =''
        item['page_url']=response.url
        item['title']=response.xpath('//div[@id="project-overview"]').xpath('.//h2/text()').extract()[0]
        item['more']=response.xpath('//title/text()').extract()[0]
        item['view_cnt']=''
        item['upload_time']=time.strftime('%Y-%m-%d',
                                          time.localtime(int(response.xpath('//ul[@class="details-list"]').xpath('.//abbr[@class="tip standard-date"]/@data-epoch')[-1].extract())))
        item['download_cnt']=''.join(re.findall('\d+',response.xpath('//ul[@class="details-list"]/li[@class="downloads"]').extract()[0]))
        item['comment_cnt']=''
        item['author']=response.xpath('//ul[@class="authors group"]/li/a/text()').extract()[0]
        yield item


