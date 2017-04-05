# -*- coding: utf-8 -*-
'''
scrapy在每个函数中，如果有循环，访问的是同一个页面，就会合并到一起。而且一定要和自己的response指定的url不一样，真是mdzz>.

'''
import sys
reload(sys)
sys.setdefaultencoding('utf-8')
import scrapy
from scrapy import Selector, Request, FormRequest
import re
import time
import decimal
import datetime
import json
from mc_bilibili_spider.items import McBilibiliSpiderItem, PublicItem
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

class mc_bilibili_spider(scrapy.Spider):
    name = 'mc_bilibili_video_information_spider'
    allowed_domains = ["bilibili.com"]
    gets = ''

    def __init__(self, choice=None):
        super(mc_bilibili_spider, self).__init__()
        self.choice = choice
        self.conn = MySQLdb.connect(host=DB_URL, user=DB_USER, passwd=DB_PSW, db=DB_NAME, charset=DB_CHARSET)
        self.cur = self.conn.cursor()

    def start_requests(self):
        if self.choice == 'create':
            urls = [
                'http://search.bilibili.com/all?keyword=%E6%88%91%E7%9A%84%E4%B8%96%E7%95%8C&page={}&order=pubdate',
                'http://search.bilibili.com/all?keyword=%E6%88%91%E7%9A%84%E4%B8%96%E7%95%8C%20minecraft&page={}&order=click',
                'http://search.bilibili.com/all?keyword=Minecraft&page={}&order=pubdate',
                'http://search.bilibili.com/all?keyword=Minecraft&page={}&order=click']
            for url in urls:
                for i in range(1, 11):
                    yield Request(url.format(i), callback=self.get_video_link)
        elif self.choice=='update':
            d = datetime.datetime.strftime(datetime.date.today() - datetime.timedelta(days=61), '%Y-%m-%d')
            sql = 'select distinct page_url from src_minecraft_public_video_day where website=%s and play_cnt>1000 and upload_time>%s'
            self.cur.execute(sql, ['bilibili', d])
            self.gets=self.cur.fetchall()
            tmp=self.gets
            for i in tmp:
                yield Request(i[0], callback=self.get_mid)
        else:
            print 'sth wrong.'

    def get_video_link(self, response):
        # 每一页的视频链接
        video_list = response.xpath('//a[@class="title"]/@href')
        urls = ['http:' + i.extract() for i in video_list]
        for url in urls:
            yield Request(url, callback=self.get_mid)

    def get_mid(self, response):
        try:
            aid = re.findall(r'(?<=av)\d+(?=/)', response.url)[0]
            mid = response.xpath('//div[@class="usname"]/a/@mid').extract()[0]
            cnt = len(re.findall(r'</option>', response.body))
            d = dict()
            d['aid'] = aid
            d['mid'] = mid
            d['cnt'] = cnt
            d['page_url'] = response.url
            url = 'http://elec.bilibili.com/web/rank/query?callback&mid={}&type=jsonp'.format(
                mid)
            yield Request(url, callback=self.get_charge_cnt, meta=d)
        except:
            print '可能是正在审核中~'
            pass

    def get_charge_cnt(self, response):
        d = response.meta
        d['item'] = McBilibiliSpiderItem()
        t = dict(json.loads(response.body.lstrip('(').rstrip(')')))
        if t["code"] == 0:
            d['item']['this_charge_cnt'] = t['data']['count']
            d['item']['history_charge_cnt'] = t['data']['total_count']
        else:
            d['item']['this_charge_cnt'] = 0
            d['item']['history_charge_cnt'] = 0
        # 这个有一大堆信息
        url = 'http://api.bilibili.com/archive_stat/stat?callback&aid={}&type=jsonp'.format(d[
                                                                                            'aid'])
        yield Request(url, callback=self.get_json, meta=d)

    def get_json(self, response):
        try:
            d = response.meta
            t = dict(json.loads(response.body))
            d['item']['coin_cnt'] = t['data']['coin']
            d['item']['collect_cnt'] = t['data']['favorite']
            d['item']['share_cnt'] = t['data']['share']
            d['item']['danmu_cnt'] = t['data']['danmaku']
            d['item']['comment_cnt'] = t['data']['reply']
            d['item']['danmu_cnt'] = t['data']['danmaku']
            d['item']['play_cnt'] = t['data']['view']
            d['item']['rank_top_daily'] = t['data']['his_rank']
            url = 'http://api.bilibili.com/vipinfo/default?callback&type=jsonp&mid={}'.format(d[
                                                                                              'mid'])
            yield Request(url, callback=self.get_post_cnt, meta=d)
        except:
            print traceback.print_exc()
            print d['item']['page_url']
            pass

    def get_post_cnt(self, response):
        try:
            d = response.meta
            t = dict(json.loads(response.body))
            d['item']['post_cnt'] = t['data']['archiveCount']
            cnt = d['cnt']
            if cnt:
                d['fenp'] = True
                for i in range(1, cnt + 1):
                    url = d['page_url'] + 'index_{}.html'.format(i)
                    yield Request(url, callback=self.get_video_information, meta=d)
            else:
                d['fenp'] = False
                url = d['page_url'] + '/'
                yield Request(url, callback=self.get_video_information, meta=d)
        except:
            print traceback.print_exc()
            print d['item']['page_url']
            pass

    def get_video_information(self, response):
        try:
            d = response.meta
            if response.meta['fenp']:
                t = response.xpath('//option')
                d['item']['page_url'] = response.url
                flag = 1
                for i in t:
                    if 'selected' in i.extract()[0]:
                        d['item']['title_p'] = i.xpath('text()').extract()[0]
                        flag = 0
                if flag:
                    d['item']['title_p'] = t[0].xpath('text()').extract()[0]
            else:
                d['item']['page_url'] = response.url[:-1]
                d['item']['title_p'] = ''
            d['item']['duration'] = str(
                int(
                    re.findall(
                        r'\d+',
                        response.xpath('//meta[@itemprop="duration"]/@content').extract()[0])[0]) * 60 + int(
                    re.findall(
                        r'\d+',
                        response.xpath('//meta[@itemprop="duration"]/@content').extract()[0])[1])) + 's'
            d['item']['crawl_time'] = time.strftime(
                '%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
            d['item']['video_url'] = 'http:' + \
                response.xpath('//meta[@itemprop="embedURL"]/@content').extract()[0]
            d['item']['title'] = response.xpath(
                '//div[@class="v-title"]/h1/text()').extract()[0]
            d['item']['category'] = '>'.join(self.check_null(
                [i.extract() for i in response.xpath('//a[@rel="v:url"]/text()')]))
            d['item']['upload_time'] = response.xpath(
                '//time[@itemprop="startDate"]/@datetime').extract()[0].split('T')[0]
            d['item']['author'] = response.xpath(
                '//div[@class="usname"]/a/@title').extract()[0]
            d['item']['label'] = ' '.join(self.check_null(
                [i.extract() for i in response.xpath('//a[@class="tag-val"]/@title')]))
            d['item']['introduction'] = self.check_null(
                response.xpath('//div[@id="v_desc"]').extract())[0]
            d['item']['personal_signiture'] = self.check_null(
                response.xpath('//div[@class="sign"]/text()').extract())[0]
            d['item']['personal_homepage'] = 'http:' + \
                response.xpath('//div[@class="usname"]/a/@href').extract()[0]
            d['item']['fans_cnt'] = re.findall(
                r'\d+',
                response.xpath('//div[@class="up-video-message"]/div[last()]/text()').extract()[0])[0]
            yield d['item']
            publicItem = PublicItem()
            publicItem['crawl_time'] = d['item']['crawl_time']
            publicItem['website'] = 'bilibili'
            publicItem['video_url'] = d['item']['video_url']
            publicItem['page_url'] = d['item']['page_url']
            publicItem['title'] = d['item']['title']
            publicItem['category'] = d['item']['category']
            publicItem['upload_time'] = d['item']['upload_time']
            publicItem['play_cnt'] = d['item']['play_cnt']
            publicItem['comment_cnt'] = d['item']['comment_cnt']
            publicItem['label'] = d['item']['label']
            publicItem['author'] = d['item']['author']
            publicItem['fans_cnt'] = d['item']['fans_cnt']
            publicItem['post_cnt'] = d['item']['post_cnt']
            publicItem['author_url'] = d['item']['personal_homepage']
            url = 'http://space.bilibili.com/ajax/member/GetInfo'
            dataform = {
                'mid': d['mid'], '_': str(self.datetime_to_timestamp_in_milliseconds(datetime.datetime.now()))}
            yield FormRequest(url, formdata=dataform,
                              callback=self.get_author_all_play_cnt, headers={'User-Agent':'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.87 Safari/537.36',
                                                                              'Referer':'http://space.bilibili.com/{}/'.format(d['mid'])}
                              ,meta={'publicItem': publicItem}, dont_filter=True)

            url = 'http://space.bilibili.com/ajax/member/getSubmitVideos?mid={}&pagesize=30&page={}&order=senddate'
            for i in range(1, 11):
                yield Request(url.format(d['mid'], i), callback=self.get_author_video)

        except:
            print traceback.print_exc()
            print d['item']['page_url']
            pass

    def get_author_all_play_cnt(self, response):
        d = json.loads(response.body)
        publicItem = response.meta['publicItem']
        publicItem['all_play_cnt'] = d['data']['playNum']
        print publicItem
        yield publicItem

    def get_author_video(self, response):
        d = json.loads(response.body)
        if 'vlist' in d['data'].keys():
            url = 'http://www.bilibili.com/video/av{}/'
            for i in d['data']['vlist']:
                yield Request(url.format(i['aid']), callback=self.get_mid)

    def nowanyi(self, item):
        if '万' in str(item):
            return int(float(item[:-1]) * 10000)
        elif '亿' in str(item):
            return int(float(item[:-1]) * 100000000)
        else:
            return item

    def check_null(self, item):
        if len(item) == 0:
            return ['']
        else:
            return item

    def datetime_to_timestamp_in_milliseconds(self,d):
        current_milli_time = lambda: int(round(time.time() * 1000))
        return current_milli_time()