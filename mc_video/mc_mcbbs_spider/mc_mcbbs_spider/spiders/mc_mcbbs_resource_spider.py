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
from mc_mcbbs_spider.items import McMcbbsSpiderItem
import traceback
import os
import MySQLdb

class mc_mcbbs_spider(scrapy.Spider):
    name = 'mc_mcbbs_resource_spider'

    def start_requests(self):
        urls=[('http://mcbbs.tvt.im/forum.php?mod=forumdisplay&fid=44&page={}','Texture Packs'),
              ('http://mcbbs.tvt.im/forum.php?mod=forumdisplay&fid=45&pages={}','Mods'),
              ('http://mcbbs.tvt.im/forum.php?mod=forumdisplay&fid=137&page={}','Maps'),
              ('http://mcbbs.tvt.im/forum.php?mod=forumdisplay&fid=801&page={}','Maps'),
              ('http://mcbbs.tvt.im/forum.php?mod=forumdisplay&fid=169&page={}','skins'),
              ('http://mcbbs.tvt.im/forum.php?mod=forumdisplay&fid=138&page={}','tools')]
        for i in urls:
            for j in range(1,5):
                yield Request(i[0].format(j),callback=self.get_each_tiezi,meta={'category':i[1]})

    def get_each_tiezi(self,response):
        fid=re.findall('(?<=&fid=)\d+',response.url)[0]
        for tbody in response.xpath('//table[@summary="forum_{}"]/tbody[@id="separatorline"]/following-sibling::tbody'.format(fid)):
            url='http://mcbbs.tvt.im/'+tbody.xpath('.//th[@class="new"]/a/@href').extract()[0]
            yield Request(url,callback=self.get_detail,meta=response.meta,dont_filter=True)

    def get_detail(self,response):
        if response.xpath('//div[@class="typeoption"]'):
            item=McMcbbsSpiderItem()
            item['crawl_time'] = time.strftime(
                '%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
            item['website']='mcbbs'
            item['category']=response.meta['category']
            item['page_url']=response.url
            item['title']=response.xpath('//title/text()').extract()[0]
            try:
                item['upload_time']=datetime.datetime(*map(lambda x:int(x),re.findall(r'\d+',
                                                                                 response.xpath('//em[starts-with(@id,"authorposton")]')[0].xpath('.//text()').
                                                                                 extract()[0])[:3])).strftime('%Y-%m-%d')
            except:
                item['upload_time'] = datetime.datetime(*map(lambda x: int(x), re.findall(r'\d+',
                                                                                          response.xpath(
                                                                                              '//em[starts-with(@id,"authorposton")]/span/@title').
                                                                                          extract()[0])[:3])).strftime(
                    '%Y-%m-%d')
            item['download_cnt']=''
            item['comment_cnt']=response.xpath('//td[@class="pls ptm pbm"]/div[@class="hm"]/span[@class="xi1"]/text()').extract()[-1]
            item['author']=response.xpath('//div[@class="authi"]/a/text()').extract()[0]
            item['view_cnt']=response.xpath('//td[@class="pls ptm pbm"]/div/span[@class="xi1"]/text()').extract()[-2]
            t=response.xpath('//div[@class="typeoption"]')
            more={'catption':t.xpath('.//caption/text()').extract()[0]}
            for i in t.xpath('.//tbody/tr'):
                more[i.xpath('.//th/text()').extract()[0][:-1]]=i.xpath('.//td/text()').extract()[0]
            item['more']=json.dumps(more)
            if more['catption']==u'地图发布':
                item['name'] = more[u'地图名字']
                item['version'] = ''
            elif more['catption']==u'材质包发布':
                item['name'] = more[u'名称']
                item['version'] = ''
            elif more['catption']==u'资源包发布':
                item['name'] = more[u'名称']
                item['version'] = more[u'适用版本']
            elif more['catption']==u'独立Mod发布' or more['catption']==u'附属Mod发布':
                item['name'] = more[u'中文名称']
                item['version'] = more[u'适用版本']
            elif more['catption']==u'皮肤发布':
                item['name'] = more[u'皮肤名称']
                item['version'] = ''
            elif more['catption']==u'服务端插件':
                item['name'] = more[u'中文名称']
                item['version'] = ''
            else:
                print more['catption']
                print more
            yield item



