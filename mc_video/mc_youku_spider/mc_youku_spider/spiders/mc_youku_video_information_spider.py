# -*- coding: utf-8 -*-
'''
scrapy不能重复页面

'''
import sys
reload(sys)
sys.setdefaultencoding('utf-8')
import os
import scrapy
import MySQLdb
from scrapy import Selector, Request
import re
import time
import datetime
import json
from mc_youku_spider.items import McYoukuSpiderItem,PublicItem
from mc_youku_spider.pipelines import McYoukuSpiderPipeline
import traceback
if "fengyufei" in ''.join(os.uname()):
    DB_URL = "127.0.0.1"
    DB_USER = "root"
    DB_PSW = "fyf!!961004"
    DB_NAME = "mc_video"
    DB_CHARSET = "utf8"

class mc_youku_spider(scrapy.Spider):
    gets=''
    name = 'mc_youku_video_information_spider'
    allowed_domains = ["youku.com"]

    def __init__(self,choice=None):
        super(mc_youku_spider,self).__init__()
        self.choice=choice
        self.conn = MySQLdb.connect(host=DB_URL, user=DB_USER, passwd=DB_PSW, db=DB_NAME, charset=DB_CHARSET)
        self.cur = self.conn.cursor()

    def start_requests(self):
        if self.choice=='create':
            # 优酷搜索，最新和最热只能看到前98页。
            # 但是第一页有60个，也就是异步加载的60页。
            urls = [
                'http://www.soku.com/search_video_ajax/q_%E6%88%91%E7%9A%84%E4%B8%96%E7%95%8C_orderby_3_limitdate_0?site=14&_lg=10&page={}',
                'http://www.soku.com/search_video_ajax/q_%E6%88%91%E7%9A%84%E4%B8%96%E7%95%8C_orderby_2_limitdate_0?site=14&_lg=10&page={}',
                'http://www.soku.com/search_video_ajax/q_minecraft_orderby_3_limitdate_0?site=14&_lg=10&page={}',
                'http://www.soku.com/search_video_ajax/q_minecraft_orderby_2_limitdate_0?site=14&_lg=10&page={}']
            for i in range(1, 11):
                for j in urls:
                    yield Request(j.format(i), callback=self.get_each_video)
        elif self.choice=='update':
            sql='select distinct page_url,duration,author,upload_time from src_minecraft_youku_video_day'
            self.cur.execute(sql)
            self.gets=self.cur.fetchall()
            for i in self.gets():
                item = McYoukuSpiderItem()
                item['crawl_time'] = time.strftime(
                    '%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
                item['page_url']=i[0]
                item['duration'] = i[1]
                item['author'] = i[2]
                item['upload_time'] = i[3]
                idd=re.findall(r'(?<=id_)[^.=]+',item['page_url'])[0]
                yield Request(item['page_url'], callback=self.get_video_information, meta={'item': item, 'id': idd})
        else:
            print 'sth wrong.'

    def get_each_video(self, response):
        for i in response.xpath('//div[@class="v"]'):
            item = McYoukuSpiderItem()
            t = re.findall(
                r'\d+',
                i.xpath('.//div[@class="v-thumb-tagrb"]/span[@class="v-time"]/text()').extract()[0])
            if len(t) == 2:
                item['duration'] = str(int(t[0]) * 60 + int(t[1])) + 's'
            else:
                item['duration'] = str(
                    int(t[0]) * 3600 + int(t[1]) * 60 + int(t[2])) + 's'
            item['crawl_time'] = time.strftime(
                '%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
            idd = i.xpath(
                'div[@class="v-meta va"]/div[@class="v-meta-title"]/a[1]/@_log_vid').extract()[0]
            item['page_url'] = 'http:' + i.xpath(
                'div[@class="v-meta va"]/div[@class="v-meta-title"]/a[1]/@href').extract()[0]
            item['author'] = i.xpath(
                './/span[@class="username"]/a[1]/text()').extract()[0].strip()
            item['upload_time'] = self.get_now(i.xpath(
                './/div[@class="v-meta-entry"]/div[last()]/span[last()]/text()').extract()[0])
            yield Request(item['page_url'], callback=self.get_video_information, meta={'item': item, 'id': idd})

    def get_video_information(self, response):
        try:
            item = response.meta['item']
            item['title']=response.xpath('//title/text()').extract()[0]
            item[
                'video_url'] = 'http://player.youku.com/player.php/sid/{}/v.swf'.format(response.meta['id'])
            try:
                item['category'] = response.xpath(
                    '//h1[@class="title"]/a[1]/text()').extract()[0]
            except:
                # 有的视频没有分类
                item['category'] = ''
            t = response.xpath('//script')[-9].extract()
            videoId = re.findall(r'(?<=videoId:")\d+(?=")', t)[0]
            videoOwner = re.findall(r'(?<=videoOwner:")\d+(?=")', t)[0]
            url = 'http://v.youku.com/action/getVideoPlayInfo?beta&vid={}' \
                '&showid=0&param%5B%5D=share&param%5B%5D=favo&param%5B%5D=download&param%5B%5D=phonewatch&param%5B%5D=updown&callback=t'
            yield Request(url.format(videoId), callback=self.get_json1, meta={'item': item, 'videoId': videoId, 'videoOwner': videoOwner})
        except:
            print response.url, '视频已丢失，请确认。'

    def get_json1(self, response):
        # 点赞数和踩数
        d = json.loads(response.body[14:-2])
        item = response.meta['item']
        item['thumb_cnt'] = d['data']['updown']['up']
        item['down_cnt'] = d['data']['updown']['down']
        item['play_cnt']=d['data']['stat']['vv']
        url = 'http://v.youku.com/action/sub?beta&callback=jQuery&vid={}&ownerid={}&showid=0&addtion=1_1&pm=1'.format(
            response.meta['videoId'], response.meta['videoOwner'])
        yield Request(url, callback=self.get_json2, meta={'item': item, 'videoId': response.meta['videoId'], 'videoOwner': response.meta['videoOwner']})

    def get_json2(self, response):
        # 作者个人信息
        d = json.loads(response.body[24:-2])
        item = response.meta['item']
        item['personal_signiture'] = d['data']['description']
        item['personal_homepage'] = 'http://i' + d['data']['url'][3:]
        item['personal_channel'] ='u','http://i' + d['data']['channelurl'][3:]
        item['fans_cnt'] = d['data']['subcount']
        url = 'http://p.comments.youku.com/ycp/comment/pc/commentList?jsoncallback=1&app=100-DDwODVkv' \
            '&objectId={}&objectType=1&listType=0&currentPage=1&pageSize=30&sign=c73dbfe2b157612d7bd6234bb166a2ed&time=1487121773'.format(response.meta['videoId'])
        yield Request(url, callback=self.get_json3, meta={'item': item})

    def get_json3(self, response):
        # 获得评论数
        item = response.meta['item']
        d = json.loads(response.body[4:-1])
        item['comment_cnt'] = d['data']['totalSize']
        yield item
        publicItem=PublicItem()
        publicItem['crawl_time'] = item['crawl_time']
        publicItem['website'] = 'youku'
        publicItem['video_url'] = item['video_url']
        publicItem['page_url'] = item['page_url']
        publicItem['title'] = item['title']
        publicItem['category'] = item['category']
        publicItem['upload_time'] = item['upload_time']
        publicItem['play_cnt'] = item['play_cnt']
        publicItem['comment_cnt'] = item['comment_cnt']
        publicItem['label'] = ''
        publicItem['author'] = item['author']
        publicItem['fans_cnt'] = item['fans_cnt']
        yield Request(item['personal_homepage'],callback=self.get_author_information,meta={'publicItem':publicItem},dont_filter=True)

    def get_author_information(self,response):
        publicItem=response.meta['publicItem']
        publicItem['post_cnt'] = response.xpath('//li[@class="snum"]/@title').extract()[0].replace(',','')
        publicItem['all_play_cnt']=response.xpath('//li[@class="vnum"]/@title').extract()[0].replace(',','')
        yield publicItem
        base_url=response.url+'/videos?page={}'
        for i in range(1,11):
            yield Request(base_url.format(i),callback=self.get_author_video,meta={'author':publicItem['author']})

    def get_author_video(self,response):
        for div in response.xpath('//div[@class="items"]/div[@class="v va"]'):
            url=div.xpath('.//div[@class="v-meta-title"]/a/@href').extract()[0]
            item = McYoukuSpiderItem()
            t = re.findall(
                r'\d+',
                    div.xpath('.//span[@class="v-time"]/text()').extract()[0])
            if len(t) == 2:
                item['duration'] = str(int(t[0]) * 60 + int(t[1])) + 's'
            else:
                item['duration'] = str(
                    int(t[0]) * 3600 + int(t[1]) * 60 + int(t[2])) + 's'
            item['crawl_time'] = time.strftime(
                '%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
            idd = re.findall(r'(?<=id_)[^.]+',url)[0]+'=='
            item['page_url'] = url
            item['author'] = response.meta['author']
            item['upload_time'] = div.xpath('.//span[@class="v-publishtime"]/text()').extract()[0]
            yield Request(url, callback=self.get_video_information,meta={'item': item, 'id': idd})


    def change(self, item):
        item = str(item)
        if len(item) < 2:
            item = '0' + item
        return item

    def get_now(self, item):
        if '年' in item:
            delta = datetime.timedelta(days=365)
            n = int(re.findall(r'\d+', item)[0])
            ddd = datetime.datetime.now() - n * delta
            return '{}-{}-{} {}:{}:{}'.format(
                self.change(
                    ddd.year), self.change(
                    ddd.month), self.change(
                    ddd.day), self.change(
                    ddd.hour), self.change(
                        ddd.minute), self.change(
                            ddd.second))
        elif '月' in item:
            delta = datetime.timedelta(days=30)
            n = int(re.findall(r'\d+', item)[0])
            ddd = datetime.datetime.now() - n * delta
            return '{}-{}-{} {}:{}:{}'.format(
                self.change(
                    ddd.year), self.change(
                    ddd.month), self.change(
                    ddd.day), self.change(
                    ddd.hour), self.change(
                        ddd.minute), self.change(
                            ddd.second))
        elif '天' in item:
            delta = datetime.timedelta(days=1)
            n = int(re.findall(r'\d+', item)[0])
            ddd = datetime.datetime.now() - n * delta
            return '{}-{}-{} {}:{}:{}'.format(
                self.change(
                    ddd.year), self.change(
                    ddd.month), self.change(
                    ddd.day), self.change(
                    ddd.hour), self.change(
                        ddd.minute), self.change(
                            ddd.second))
        elif '小时' in item:
            delta = datetime.timedelta(hours=1)
            n = int(re.findall(r'\d+', item)[0])
            ddd = datetime.datetime.now() - n * delta
            return '{}-{}-{} {}:{}:{}'.format(
                self.change(
                    ddd.year), self.change(
                    ddd.month), self.change(
                    ddd.day), self.change(
                    ddd.hour), self.change(
                        ddd.minute), self.change(
                            ddd.second))
        elif '分钟' in item:
            delta = datetime.timedelta(minutes=1)
            n = int(re.findall(r'\d+', item)[0])
            ddd = datetime.datetime.now() - n * delta
            return '{}-{}-{} {}:{}:{}'.format(
                self.change(
                    ddd.year), self.change(
                    ddd.month), self.change(
                    ddd.day), self.change(
                    ddd.hour), self.change(
                        ddd.minute), self.change(
                            ddd.second))
        elif '秒' in item:
            delta = datetime.timedelta(seconds=1)
            n = int(re.findall(r'\d+', item)[0])
            ddd = datetime.datetime.now() - n * delta
            return '{}-{}-{} {}:{}:{}'.format(
                self.change(
                    ddd.year), self.change(
                    ddd.month), self.change(
                    ddd.day), self.change(
                    ddd.hour), self.change(
                        ddd.minute), self.change(
                            ddd.second))

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