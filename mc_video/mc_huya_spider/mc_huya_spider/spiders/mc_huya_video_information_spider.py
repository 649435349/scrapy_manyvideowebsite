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
from mc_huya_spider.items import McHuyaSpiderItem
import traceback
import os
import MySQLdb

if "fengyufei" in ''.join(os.uname()):
    DB_URL = "127.0.0.1"
    DB_USER = "root"
    DB_PSW = "fyf!!961004"
    DB_NAME = "mc_video"
    DB_CHARSET = "utf8"

class mc_huya_spider(scrapy.Spider):
    name = 'mc_huya_video_information_spider'
    allowed_domains = ["huya.com"]
    gets = ''

    def __init__(self, choice=None):
        super(mc_huya_spider, self).__init__()
        self.choice = choice
        self.conn = MySQLdb.connect(host=DB_URL, user=DB_USER, passwd=DB_PSW, db=DB_NAME, charset=DB_CHARSET)
        self.cur = self.conn.cursor()

    def start_requests(self):
        if self.choice=='create':
            # 抓前20页意思意思
            urls = [
                'http://v.huya.com/index.php?r=search/index&p={}&order=news&w=%E6%88%91%E7%9A%84%E4%B8%96%E7%95%8C',
                'http://v.huya.com/index.php?r=search/index&p={}&order=play&w=%E6%88%91%E7%9A%84%E4%B8%96%E7%95%8C',
                'http://v.huya.com/index.php?r=search/index&p={}&order=news&w=minecraft',
                'http://v.huya.com/index.php?r=search/index&p={}&order=play&w=minecraft', ]
            for i in range(1, 21):
                for j in urls:
                    yield Request(j.format(i), callback=self.get_each_video)
        elif self.choice=='update':
            sql='select distinct page_url from src_minecraft_public_video_day where website=()'
            self.cur.execute(sql,('huya'))
            self.gets=self.cur.fetchall()
            for i in self.gets():
                yield Request(i[0], callback=self.get_video_information)
        else:
            print 'sth wrong.'


    def get_each_video(self, response):
        for li in response.xpath('//ul[@class="video-list tab-cont"]/li'):
            url = li.xpath('.//a[1]/@href').extract()[0]
            yield Request(url, callback=self.get_video_information)

    def get_video_information(self, response):
        item = McHuyaSpiderItem()
        item['crawl_time'] = time.strftime(
            '%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
        item['website'] = 'huya'
        vid = re.findall(r'(?<=play/)\d+', response.url)[0]
        item[
            'video_url'] = 'http://vhuya.dwstatic.com/video/vppp.swf?uu=a04808d307&vu=&channelId=mc&auto_play=1&sdk=dw&vid={}&logo=huya&no_danmu=1&from=vhuyashareweb'.format(vid)
        item['page_url'] = response.url
        item['title'] = response.xpath(
            '//h1[@class="play-title"]/text()').extract()[0]
        item['category'] = '>'.join(response.xpath(
            '//div[@class="crumbs"]/*/text()').extract())
        item['play_cnt'] = self.nowanyi(response.xpath(
            '//div[@class="play-video-num"]/em/text()')[0])
        item['comment_cnt'] = re.findall(
            r'\d+', response.xpath('//a[@class="comment"]').extract()[0])[0]
        item['label'] = response.xpath('//meta[@name="Keywords"]/@content')
        item['author'] = response.xpath(
            '//div[@class="video-author-info-detail"]/p/a/text()').extract()[0]
        url = 'http://m.v.huya.com/play/{}.html'.format(vid)
        urll = response.xpath(
            '//div[@class="video-col-title-link"]/a/@href').extract()[0]
        yield Request(url, callback=self.get_uploadtime, meta={'item': item, 'urll': urll})

    def get_uploadtime(self, response):
        item = response.meta['item']
        ds = re.findall(r'\d+', response.xpath('//blockquote').extract()[0])
        item['upload_time'] = ds[0] + '-' + ds[1] + '-' + \
            ds[2] + ' ' + ds[3] + ':' + ds[4] + ':' + '00'
        yield Request(response.meta['urll'], callback=self.get_author_information, meta={'item': item, 'urll': response.meta['urll']}, dont_filter=True)

    def get_author_information(self, response):
        item = response.meta['item']
        item['fans_cnt'] = self.nowanyi(response.xpath(
            '//li[@class="first-li"]/span/text()').extract()[0])
        item['post_cnt'] = self.nowanyi(response.xpath(
            '//div[@class="stat"]/ul/li[2]/span/text()').extract()[0])
        item['all_play_cnt'] = self.nowanyi(response.xpath(
            '//div[@class="stat"]/ul/li[4]/span/text()').extract()[0])
        yield item
        for i in range(20):
            yield Request(response.meta['urll'] + '?p={}'.format(i), callback=self.get_author_video, meta={'urll': response.meta['urll']})

    def get_author_video(self, response):
        if response.xpath('//ul[@class="video-list fltL"]/li'):
            for li in response.xpath('//ul[@class="video-list fltL"]/li'):
                url = li.xpath('.//a[1]/@href').extract()[0]
                yield Request(url, callback=self.get_video_information)

    def nowanyi(self, item):
        if '千万' in str(item):
            return int(float(item[:-2]) * 10000000)
        elif '百万' in str(item):
            return int(float(item[:-2]) * 1000000)
        elif '万' in str(item):
            return int(float(item[:-1]) * 10000)
        elif '亿' in str(item):
            return int(float(item[:-1]) * 100000000)
        else:
            return item
