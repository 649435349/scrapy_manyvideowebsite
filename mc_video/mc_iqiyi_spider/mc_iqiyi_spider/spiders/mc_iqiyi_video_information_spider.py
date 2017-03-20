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
from mc_iqiyi_spider.items import McIqiyiSpiderItem, PublicItem
import traceback
import os
import MySQLdb

if "fengyufei" in ''.join(os.uname()):
    DB_URL = "127.0.0.1"
    DB_USER = "root"
    DB_PSW = "fyf!!961004"
    DB_NAME = "mc_video"
    DB_CHARSET = "utf8"


class mc_iqiyi_spider(scrapy.Spider):
    name = 'mc_iqiyi_video_information_spider'
    allowed_domains = ["iqiyi.com"]
    gets = ''

    def __init__(self, choice=None):
        super(mc_iqiyi_spider, self).__init__()
        self.choice = choice
        self.conn = MySQLdb.connect(
            host=DB_URL,
            user=DB_USER,
            passwd=DB_PSW,
            db=DB_NAME,
            charset=DB_CHARSET)
        self.cur = self.conn.cursor()

    def start_requests(self):
        if self.choice == 'create':
            # 爱奇艺最多20页
            urls = [
                'http://so.iqiyi.com/so/q_%E6%88%91%E7%9A%84%E4%B8%96%E7%95%8C_ctg__t_0_page_{}_p_1_qc_0_rd__site_iqiyi_m_4_bitrate_?af=true',
                'http://so.iqiyi.com/so/q_%E6%88%91%E7%9A%84%E4%B8%96%E7%95%8C_ctg__t_0_page_{}_p_1_qc_0_rd__site_iqiyi_m_11_bitrate_?af=true#',
                'http://so.iqiyi.com/so/q_minecraft_ctg__t_0_page_{}_p_1_qc_0_rd__site__m_4_bitrate_',
                'http://so.iqiyi.com/so/q_minecraft_ctg__t_0_page_{}_p_1_qc_0_rd__site__m_11_bitrate_']
            for i in range(1, 21):
                for j in urls:
                    yield Request(j.format(i), callback=self.get_each_video)

        elif self.choice == 'update':
            sql = 'select distinct page_url from src_minecraft_public_video_day where website=()'
            self.cur.execute(sql, ('iqiyi'))
            self.gets = self.cur.fetchall()
            for i in self.gets():
                item = McIqiyiSpiderItem()
                item['crawl_time'] = time.strftime(
                    '%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
                item['page_url'] = i[0]
                heji = True if 'v_' in i[0] else False
                yield Request(i[0], callback=self.get_video_information, meta={'item': item, 'heji': heji})
        else:
            print 'sth wrong.'

    def get_each_video(self, response):
        for i in response.xpath('//li[@class="list_item"]'):
            try:
                albumId = i.xpath(
                    '@data-widget-searchlist-albumid').extract()[0]
                tvId = i.xpath('@data-widget-searchlist-tvid').extract()[0]
                tt = i.xpath('.//h3[@class="result_title"]')
                t = tt.xpath('.//a[1]')
                url = t.xpath('@href').extract()[0]
                if albumId == 0:  # 是外链接
                    print url, '外链视频不抓取'
                elif albumId != tvId or ('playlist' in url):
                    yield Request(url, callback=self.heji, meta={'heji': True})
                elif albumId == tvId:  # 是单视
                    item = McIqiyiSpiderItem()
                    item['crawl_time'] = time.strftime(
                        '%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
                    item['page_url'] = url
                    yield Request(url, callback=self.get_video_information, meta={'item': item, 'heji': False})
                else:
                    print '?????????????????'
            except:
                print i.extract()

    def heji(self, response):
        # 处理重定向
        if response.xpath('//div'):
            yield Request(response.url, callback=self.heji2, meta=response.meta, dont_filter=True)
        else:
            url = re.findall(r'(?<=URL=\')[^\']*(?=\')', response.body)[0]
            yield Request(url, callback=self.heji2, meta=response.meta)

    def heji2(self, response):
        try:
            url=response.xpath('//a[@class="textOverflow"]/@href').extract()[2]+'/v?page={}&video_type=1'
        except:
            url=response.xpath('//p[@class="name-r textOverflow"]/a/@href').extract()[0]
        for i in range(1, 21):
            yield Request(url.format(i), callback=self.get_author_video)
    '''
    def heji3(self, response):
        meta = response.meta
        d = json.loads(response.body[6:-15])
        for i in d['data']:
            item = McIqiyiSpiderItem()
            item['crawl_time'] = time.strftime(
                '%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
            item['page_url'] = i['vUrl']
            item['topic'] = i['sName']
            item['upload_time'] = i['tvYear']
            q = meta
            q['item'] = item
            q['vid'] = i['vid']
            q['tvId'] = i['tvId']
            yield Request(i['vUrl'], callback=self.get_video_information, meta=q)
    '''
    def get_video_information(self, response):
        meta = response.meta
        item = meta['item']
        try:
            albumId = re.findall(r'(?<=albumId:\s)\d+', response.body)[0]
            tvId = re.findall(r'(?<=tvId:\s)\d+', response.body)[0]
        except:
            albumId = re.findall(r'(?<=albumId:)\d+', response.body)[0]
            tvId = re.findall(r'(?<=tvId:)\d+', response.body)[0]
        item['introduction'] = self.check_null(response.xpath(
            '//meta[@name="description"]/@content').extract())
        item['title'] = response.xpath(
            '//meta[@name="irTitle"]/@content').extract()[0]
        item['tag'] = ' '.join(response.xpath(
            '//span[@id="widget-videotag"]').xpath('a/@title').extract())
        if 'w_' in response.url:
            item['upload_time'] = '-'.join(re.findall(r'\d+', response.xpath(
                '//p[@id="widget-vshort-ptime"]/text()').extract()[0]))
            item['topic'] = ''
            meta['vid'] = response.xpath(
                '//div[@data-player-videoid]/@data-player-videoid').extract()[0]
        try:
            ps = re.findall(r'\d+', item['upload_time'])
            item['video_url'] = 'http://www.iqiyi.com/common/flashplayer/' \
                '{}/10211a7ff0f5.swf?vid={}' \
                '&pageURL={}.swf&albumId={}&tvId={}'.format(
                ''.join(ps[:3]), meta['vid'], re.findall(r'(?<=iqiyi\.com/)[^.]*(?=\.html)', response.url)[0], albumId, tvId
            )
            meta['item'] = item
            url = 'http://mixer.video.iqiyi.com/jp/mixin/videos/{}?callback=1'.format(
                tvId)
            yield Request(url, callback=self.get_json, meta=meta, dont_filter=True)
        except:
            print '视频在合集里面已经抓取', item['page_url']

    def get_json(self, response):
        # 视频信息
        meta = response.meta
        item = meta['item']
        d = json.loads(response.body[6:-15])
        item['category'] = '>'.join([i['title'] for i in d['crumbList']])
        item['play_cnt'] = d['playCount']
        item['comment_cnt'] = d['commentCount']
        item['duration'] = str(d['duration']) + 's'
        item['author'] = d['user']['name']
        item['up_cnt'] = str(d['upCount']) + '/' + str(d['downCount'])
        item['vote_cnt'] = len(d['votes'])
        meta['item'] = item
        userId = d['userId']
        meta['userId'] = userId
        item['personal_homepage'] = 'http://www.iqiyi.com/u/{}'.format(userId)
        url = 'http://query.reward.iqiyi.com/play/queryUserReward.action?uid={}&showNum=4&callback=1'.format(
            userId)
        yield Request(url, callback=self.get_json2, meta=meta, dont_filter=True)

    def get_json2(self, response):
        meta = response.meta
        item = meta['item']
        d = json.loads(response.body[13:-14])
        item['reward_cnt'] = d['data']['data']['total']
        item['personal_signiture'] = d['data']['data']['seekWord']
        publicItem = PublicItem()
        publicItem['crawl_time'] = item['crawl_time']
        publicItem['website'] = 'iqiyi'
        publicItem['video_url'] = item['video_url']
        publicItem['page_url'] = item['page_url']
        publicItem['title'] = item['title']
        publicItem['category'] = item['category']
        publicItem['upload_time'] = item['upload_time']
        publicItem['play_cnt'] = item['play_cnt']
        publicItem['comment_cnt'] = item['comment_cnt']
        publicItem['label'] = item['tag']
        publicItem['author'] = item['author']
        yield item
        url = 'http://www.iqiyi.com/u/{}/v'.format(meta['userId'])
        yield Request(url, callback=self.get_author_information, meta={'publicItem': publicItem, 'userId': response.meta['userId']}, dont_filter=True)

    def get_author_information(self, response):
        publicItem = response.meta['publicItem']
        try:
            publicItem['fans_cnt'] = response.xpath(
                '//div[@class="info_connect"]').xpath('.//em[@class="count"]/a/@data-countnum').extract()[0]
            publicItem['all_play_cnt'] = self.nowanyi(response.xpath(
                '//div[@class="info_connect"]').xpath('.//em[@class="count"]/a/text()').extract()[0])
            publicItem['post_cnt'] = self.nowanyi(response.xpath(
                '//div[@class="info_connect"]').xpath('.//em[@class="count"]/a/text()').extract()[0])
            yield publicItem
        except:
            try:
                publicItem['fans_cnt'] = self.nowanyi(re.findall(r'\d+', response.xpath(
                    '//span[@class="f14 ml10"]').extract()[-1])[0])
            except:
                print '没有粉丝数',response.url
                publicItem['fans_cnt'] =''
            publicItem['all_play_cnt'] = self.nowanyi(response.xpath(
                '//div[@class="num_item"]').xpath('.//i')[1].extract()[0].replace(',', ''))
            publicItem['post_cnt'] = self.nowanyi(response.xpath(
                '//div[@class="num_item"]').xpath('.//i')[0].extract()[0].replace(',', ''))
            yield publicItem
        url = 'http://www.iqiyi.com/u/{}/v?page={}&video_type=1'
        for i in range(1, 21):
            yield Request(url.format(response.meta['userId'], i), callback=self.get_author_video)

    def get_author_video(self, response):
        for li in response.xpath('//ul/li'):
            try:
                url = li.xpath(
                    './/p[@class="site-piclist_info_title_twoline"]/a/@href').extract()[0]
                item = McIqiyiSpiderItem()
                item['crawl_time'] = time.strftime(
                    '%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
                item['page_url']=url
                heji = True if 'v_' in url else False
                yield Request(url, callback=self.get_video_information,meta={'item':item,'heji':heji})
            except:
                continue

    def check_null(self, item):
        if len(item) == 0:
            return ['']
        else:
            return item[0]

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
