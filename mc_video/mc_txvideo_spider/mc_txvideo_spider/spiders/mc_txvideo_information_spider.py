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
from mc_txvideo_spider.items import McTxvideoSpiderItem, PublicItem
import traceback
import os
import MySQLdb

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
    name = 'mc_txvideo_video_information_spider'
    allowed_domains = ["qq.com"]
    gets = ''

    def __init__(self, choice=None):
        super(mc_acfun_spider, self).__init__()
        self.choice = choice
        self.conn = MySQLdb.connect(host=DB_URL, user=DB_USER, passwd=DB_PSW, db=DB_NAME, charset=DB_CHARSET)
        self.cur = self.conn.cursor()

    def start_requests(self):
        if self.choice == 'create':
            # 腾讯视频最新和最热最多有20页。
            urls = ['https://v.qq.com/x/search/?ses=qid%3D5_jowT0Du8Dwxa9gzoop8dD5kcibr-KjcwqWqXsebQmQzxlSQuUUHA%26last_query%3Dminecraft%26tabid_list%3D0%7C2%7C1%7C17%7C15%7C7%26tabname_list%3D%E5%85%A8%E9%83%A8%7C%E7%94%B5%E8%A7%86%E5%89%A7%7C%E7%94%B5%E5%BD%B1%7C%E6%B8%B8%E6%88%8F%7C%E6%95%99%E8%82%B2%7C%E5%85%B6%E4%BB%96&q=minecraft&stag=3&cur={}&cxt=tabid%3D0%26sort%3D1%26pubfilter%3D0%26duration%3D0',
                    'https://v.qq.com/x/search/?ses=qid%3D5_jowT0Du8Dwxa9gzoop8dD5kcibr-KjcwqWqXsebQmQzxlSQuUUHA%26last_query%3Dminecraft%26tabid_list%3D0%7C2%7C1%7C17%7C15%7C7%26tabname_list%3D%E5%85%A8%E9%83%A8%7C%E7%94%B5%E8%A7%86%E5%89%A7%7C%E7%94%B5%E5%BD%B1%7C%E6%B8%B8%E6%88%8F%7C%E6%95%99%E8%82%B2%7C%E5%85%B6%E4%BB%96&q=minecraft&stag=3&cur={}&cxt=tabid%3D0%26sort%3D2%26pubfilter%3D0%26duration%3D0',
                    'https://v.qq.com/x/search/?ses=qid%3DnGTlIcIZ0avBA7bjmTfI6b2poPX4bevhYoLzp3bAVxLF216Cg159-Q%26last_query%3D%E6%88%91%E7%9A%84%E4%B8%96%E7%95%8C%26tabid_list%3D0%7C2%7C1%7C17%7C15%7C7%26tabname_list%3D%E5%85%A8%E9%83%A8%7C%E7%94%B5%E8%A7%86%E5%89%A7%7C%E7%94%B5%E5%BD%B1%7C%E6%B8%B8%E6%88%8F%7C%E6%95%99%E8%82%B2%7C%E5%85%B6%E4%BB%96&q=%E6%88%91%E7%9A%84%E4%B8%96%E7%95%8C&stag=3&cur={}&cxt=tabid%3D0%26sort%3D1%26pubfilter%3D0%26duration%3D0',
                    'https://v.qq.com/x/search/?ses=qid%3DnGTlIcIZ0avBA7bjmTfI6b2poPX4bevhYoLzp3bAVxLF216Cg159-Q%26last_query%3D%E6%88%91%E7%9A%84%E4%B8%96%E7%95%8C%26tabid_list%3D0%7C2%7C1%7C17%7C15%7C7%26tabname_list%3D%E5%85%A8%E9%83%A8%7C%E7%94%B5%E8%A7%86%E5%89%A7%7C%E7%94%B5%E5%BD%B1%7C%E6%B8%B8%E6%88%8F%7C%E6%95%99%E8%82%B2%7C%E5%85%B6%E4%BB%96&q=%E6%88%91%E7%9A%84%E4%B8%96%E7%95%8C&stag=3&cur={}&cxt=tabid%3D0%26sort%3D2%26pubfilter%3D0%26duration%3D0']
            for i in urls:
                for j in range(1, 6):
                    yield Request(i.format(j), callback=self.get_each_video)
        elif self.choice=='update':
            d = datetime.datetime.strftime(datetime.date.today() - datetime.timedelta(days=61), '%Y-%m-%d')
            sql = 'select distinct page_url from src_minecraft_public_video_day where website=%s and play_cnt>1000 and upload_time>%s'
            self.cur.execute(sql, ['qq', d])
            self.gets=self.cur.fetchall()
            for i in self.gets:
                vid = re.findall(r'(?<=/)[^./]+(?=\.html)', i[0])[0]
                yield Request(i[0], callback=self.get_video_information,meta={'vid':vid})
        else:
            print 'sth wrong.'

    def get_each_video(self, response):
        # 每一页有合集视频和单个视频，区别。
        # 单个视频
        l1 = response.xpath('//div[@class="result_item result_item_h"]')
        # 合集
        l2 = response.xpath('//div[contains(@class,"result_item_v")]')
        for i in l1:
            url = i.xpath('h2[@class="result_title"]/a[1]/@href').extract()[0]
            # url='https://v.qq.com/x/page/c0374dooiui.html'
            if 'qq' in url:
                vid = re.findall(r'(?<=/)[^/.]+(?=\.html)', url)[0]
                yield Request(url, callback=self.get_video_information, meta={'vid': vid})
            # break
        for i in l2:
            # break
            # 合集在jsonp里面传过来
            # 一个合集不可能超过1000000000个视频吧我觉得。。
            idd = i.xpath('@data-id').extract()[0]
            url = 'https://s.video.qq.com/get_playsource?id={}&plat=2&type=4&data_type=3&video_type=6&plname=qq&range=1-100000000&otype=json&uid=c6318f0a-c17f-4893-a13c-1c6b044b6338&callback=1'.format(
                idd)
            yield Request(url, callback=self.get_json,)

    def get_json(self, response):
        d = json.loads(response.body[2:-1])
        for i in d['PlaylistItem']['videoPlayList']:
            if 'qq' in i['playUrl']:
                yield Request(i['playUrl'], callback=self.get_video_information, meta={'vid': re.findall(r'(?<=/)[^/.]+(?=\.html)', i['playUrl'])[0]})

    def get_video_information(self, response):
        try:
            uin = response.xpath(
                '//a[@class="btn_book"]/@r-subscribe').extract()[0]
        except:
            uin=''
        item = McTxvideoSpiderItem()
        item['crawl_time'] = time.strftime(
            '%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
        url = response.xpath('//meta[@itemprop="url"]/@content').extract()[0]
        if '/page/' in url:
            item['page_url'] = url
        else:
            item['page_url'] = url[:-5] + '/' + response.meta['vid'] + '.html'
        item['title'] = response.xpath('//title/text()').extract()[0]
        item['category'] = response.xpath(
            '//div[@class="nav_item current"]/a[1]/text()').extract()[0]
        item['video_url'] = 'https://imgcache.qq.com/tencentvideo_v1/playerv3/TPout.swf?max_age=86400&v=20161117&vid={}&auto=0'.format(response.meta[
                                                                                                                                       'vid'])

        try:
            item['author'] = response.xpath(
                '//span[@class="user_name"]/text()').extract()[0]
        except:
            item['author'] = ''
        try:
            item['personal_homepage'] = response.xpath(
                '//a[@class="user_info"]/@href').extract()[0]
        except:
            item['personal_homepage'] =''
        try:
            item['subscribe_cnt'] = self.nowanyi(response.xpath(
                '//a[@class="btn_book"]/span[@class="num"]/text()').extract()[0])
        except:
            item['subscribe_cnt'] = 0
        t = response.xpath('//div[contains(@class,"video_user")]')
        try:
            item['upload_time'] = '{}-{}-{}'.format(
                re.findall(
                    r'\d+',
                    t.xpath('span[last()]/text()').extract()[0])[0],
                re.findall(
                    r'\d+',
                    t.xpath('span[last()]/text()').extract()[0])[1],
                re.findall(
                    r'\d+',
                    t.xpath('span[last()]/text()').extract()[0])[2])
        except:
            item['upload_time'] = ''
        item['label'] = ' '.join(response.xpath(
            '//div[@class="video_tags"]/a[@class="tag_item"]/text()').extract())
        item['duration'] = response.xpath(
            '//meta[@itemprop="duration"]/@content').extract()[0] + 's'
        if '/page/' in item['page_url']:
            item['play_cnt'] = self.nowanyi(response.xpath(
                '//em[@id="mod_cover_playnum"]/text()').extract()[0])
        else:
            item['play_cnt'] = self.nowanyi(response.xpath(
                '//span[@data-id="{}"]/text()'.format(response.meta['vid'])).extract()[0])
        # comment数量来自iframe框架，先请求comment_id，在获得评论数。
        yield Request('https://ncgi.video.qq.com/fcgi-bin/video_comment_id?otype=json&callback=1&op=3&vid={}'.format(response.meta['vid']),
                      meta={'item': item, 'vid': response.meta['vid'], 'uin': uin}, callback=self.get_comment_id)

    def get_comment_id(self, response):
        d = json.loads(response.body[2:-1])
        comment_id = d['comment_id']
        # t=response.meta
        # t['comment_id']=comment_id
        yield Request('https://coral.qq.com/article/{}/hotcomment?reqnum=10&callback=1'.format(comment_id), meta=response.meta, callback=self.get_comment_cnt)

    def get_comment_cnt(self, response):
        d = json.loads(response.body[2:-1])
        item = response.meta['item']
        item['comment_cnt'] = self.nowanyi(
            d['data']['targetinfo']['commentnum'])
        publicItem = PublicItem()
        publicItem['crawl_time'] = item['crawl_time']
        publicItem['website'] = 'qq'
        publicItem['video_url'] = item['video_url']
        publicItem['page_url'] = item['page_url']
        publicItem['title'] = item['title']
        publicItem['category'] = item['category']
        publicItem['upload_time'] = item['upload_time']
        publicItem['play_cnt'] = item['play_cnt']
        publicItem['comment_cnt'] = item['comment_cnt']
        publicItem['label'] = item['label']
        publicItem['author'] = item['author']
        publicItem['fans_cnt'] = item['subscribe_cnt']
        publicItem['author_url'] = item['personal_homepage']
        yield item
        if item['personal_homepage']:
            yield Request(item['personal_homepage'], callback=self.get_author_information, meta={'publicItem': publicItem, 'uin': response.meta['uin']}, dont_filter=True)

    def get_author_information(self, response):
        publicItem = response.meta['publicItem']
        try:
            publicItem['post_cnt'] = self.nowanyi(
                response.xpath('//ul[@class="user_count"]/li[3]/span[2]/text()').extract()[0].replace(',',''))
            publicItem['all_play_cnt'] = self.nowanyi(
                response.xpath('//ul[@class="user_count"]/li[7]/span[2]/text()').extract()[0].replace(',',''))
            yield publicItem
        except:
            # 老版的腾讯视频
            publicItem['post_cnt'] = self.nowanyi(response.xpath(
                '//li[@class="user_count_video"]/span[@class="count_num"]/text()').extract()[0].replace(',',''))
            publicItem['all_play_cnt'] = self.nowanyi(response.xpath(
                '//li[@class="user_count_play"]/span[@class="count_num"]/text()').extract()[0].replace(',',''))
            yield publicItem
        print publicItem
        for i in range(1, 4):
            url = 'http://c.v.qq.com/vchannelinfo?otype=json&uin={}&qm=1&pagenum={}&num=24&callback=1'
            yield Request(url.format(response.meta['uin'], i), callback=self.get_author_video)


    def get_author_video(self, response):
        d = json.loads(response.body[2:-1])
        if 'videolst' in d.keys() and d['videolst']:
            for i in d['videolst']:
                yield Request(i['url'], callback=self.get_video_information, meta={'vid': i['vid']})

    def nowanyi(self, item):
        if '万' in str(item):
            return int(float(item[:-1]) * 10000)
        elif '亿' in str(item):
            return int(float(item[:-1]) * 100000000)
        else:
            return item
