# -*- coding: utf-8 -*-
'''
scrapy不能重复页面
'''
import sys
reload(sys)
sys.setdefaultencoding('utf-8')
import scrapy
from scrapy import Selector, Request, FormRequest
import re
from bs4 import BeautifulSoup
import json
import time
import MySQLdb
import datetime
import json
from mc_youtube_spider.items import McYoutubeSpiderItem
import traceback
import os

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

class mc_youtube_spider(scrapy.Spider):
    name = 'mc_youtube_video_information_spider'
    allowed_domains = ["youtube.com"]
    gets=''

    def __init__(self,choice=None):
        super(mc_youtube_spider,self).__init__()
        self.choice=choice
        self.conn = MySQLdb.connect(host=DB_URL, user=DB_USER, passwd=DB_PSW, db=DB_NAME, charset=DB_CHARSET)
        self.cur = self.conn.cursor()

    def start_requests(self):
        if self.choice == 'create':
            # 抓前20页意思意思
            urls = [
                'https://www.youtube.com/results?sp=CAM%253D&q=我的世界&p={}',
                'https://www.youtube.com/results?sp=CAJIAOoDAA%253D%253D&q=%我的世界C&p={}',
                'https://www.youtube.com/results?q=minecraft&sp=CAI%253D&p={}',
                'https://www.youtube.com/results?q=minecraft&sp=CAM%253D&p={}',
                   ]
            for i in range(1, 2):
                for j in urls:
                    yield Request(j.format(i), callback=self.get_each_video)
        elif self.choice=='update':
            d = datetime.datetime.strftime(datetime.date.today() - datetime.timedelta(days=61), '%Y-%m-%d')
            sql = 'select distinct page_url from src_minecraft_public_video_day where website=%s and play_cnt>1000 and upload_time>%s'
            self.cur.execute(sql, ['youtube', d])
            self.gets=self.cur.fetchall()
            for i in self.gets:
                yield Request(i[0], callback=self.get_video_information)
        else:
            print 'sth wrong.'

    def get_each_video(self, response):
        for li in response.xpath('//ol[@class="item-section"]/li'):
            url = 'https://www.youtube.com' + li.xpath('.//h3[@class="yt-lockup-title "]/a[1]/@href').extract()[0]
            if 'watch' in url:
                yield Request(url, callback=self.get_video_information)
            elif 'user' in url:
                url += '/videos'
                yield Request(url, callback=self.get_user_video)

    def get_user_video(self, response):
        for li in response.xpath(
                '//ul[@id="channels-browse-content-grid"]/li[@class="channels-content-item yt-shelf-grid-item"]'):
            url = 'https://www.youtube.com' + \
                li.xpath('.//h3[@class="yt-lockup-title "]/a[1]/@href').extract()[0]
            yield Request(url, callback=self.get_video_information)
        if response.xpath(
                '//button[@data-uix-load-more-href]/@data-uix-load-more-href'):
            next_param = response.xpath(
                '//button[@data-uix-load-more-href]/@data-uix-load-more-href').extract()[0]
            url = 'https://www.youtube.com{}'.format(next_param)
            yield Request(url, callback=self.get_user_video2)

    def get_user_video2(self, response):
        d = json.loads(response.body)
        soup_content = BeautifulSoup(d['content_html'])
        for li in soup_content.find_all(
            'li', {
                'class': 'channels-content-item yt-shelf-grid-item'}):
            url = 'https://www.youtube.com' + \
                li.find('h3', {'class': 'yt-lockup-title '}).find('a').attrs['href']
            yield Request(url, callback=self.get_video_information)
        if d['load_more_widget_html']:
            soup_next = BeautifulSoup(d['load_more_widget_html'])
            next_param = soup_next.find('button').attrs[
                'data-uix-load-more-href']
            url = 'https://www.youtube.com{}'.format(next_param)
            yield Request(url, callback=self.get_user_video2)

    def get_video_information(self, response):
        author_video_url='https://www.youtube.com'+response.xpath('//div[@class="yt-user-info"]/a/@href').extract()[0]+'/videos'
        yield Request(author_video_url,callback=self.get_user_video)
        item = McYoutubeSpiderItem()
        item['crawl_time'] = time.strftime(
            '%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
        item['website'] = 'youtube'
        v = re.findall(r'(?<=v=).*', response.url)[0]
        item['video_url'] = 'https://www.youtube.com/embed/{}'.format(v)
        item['page_url'] = response.url
        item['title'] = response.xpath('//span[@id="eow-title"]/text()').extract()[0].strip('\n ')
        item['category'] = ''
        ttmp = response.xpath(
            '//strong[@class="watch-time-text"]/text()').extract()[0]
        item[
            'upload_time'] = self.get_now(ttmp)
        if not re.findall(
            '\d+',
            response.xpath('//div[@class="watch-view-count"]/text()').extract()[0].replace(
                ',',
                '')):
            item['play_cnt'] = 0
        else:
            item['play_cnt'] = re.findall('\d+', response.xpath(
                '//div[@class="watch-view-count"]/text()').extract()[0].replace(',', ''))[0]
        item['label'] = ''
        item['author'] = response.xpath(
            '//div[@class="yt-user-info"]/a[1]/text()').extract()[0]
        try:
            item['fans_cnt'] = response.xpath(
                '//span[@class="yt-subscription-button-subscriber-count-branded-horizontal yt-subscriber-count"]/text()').extract()[0].replace(',', '')
        except:
            item['fans_cnt'] = ''
        XSRF_TOKEN = re.findall(
            r'(?<=XSRF_TOKEN\':\s")[^"]+', response.xpath('//script').extract()[-1])[0]
        COMMENTS_TOKEN = re.findall(
            r'(?<=COMMENTS_TOKEN\':\s")[^"]+', response.xpath('//script').extract()[-2])[0]
        formdata = {
            'session_token': XSRF_TOKEN,
            'client_url': response.url}
        url = 'https://www.youtube.com/watch_fragments_ajax?v={}&tr=time&distiller=1&ctoken={}&frags=comments&spf=load'.format(
            v, COMMENTS_TOKEN)
        author_url=response.xpath('//span[@itemprop="author"]/link/@href')[0].extract()
        yield FormRequest(url, callback=self.get_get1, meta={'item': item,'author_url':author_url}, formdata=formdata)

    def get_get1(self, response):
        item = response.meta['item']
        try:
            d = json.loads(response.body)
            soup = BeautifulSoup(d['body']['watch-discussion'])
            if re.findall('\d+',
                          soup.find('h2',
                                    {'class': 'comment-section-header-renderer'}).text.replace(',',
                                                                                               '').strip('(').strip(
                              ')')):
                item['comment_cnt'] = ''.join(re.findall('\d+',
                                                         soup.find('h2',
                                                                   {
                                                                       'class': 'comment-section-header-renderer'}).text.replace(
                                                             ',',
                                                             '').strip('(').strip(')')))
            else:
                item['comment_cnt'] = 0
        except:
            item['comment_cnt'] = ''
        item['post_cnt'] = ''
        url =response.meta['author_url']+'/about'
        yield Request(url, callback=self.get_get2,
                      meta={'item': item},
                      dont_filter=True)

    def get_get2(self, response):
        item = response.meta['item']
        item['author_url']=response.url
        try:
            item['all_play_cnt'] = response.xpath(
                '//div[@class="about-stats"]/span[2]/b/text()').extract()[0].replace(',', '')
        except:
            item['all_play_cnt'] = ''
        yield item

    def change_item(self, item):
        item = str(item)
        if len(item) == 1:
            return '0' + item
        return item

    def change(self, item):
        item = str(item)
        if len(item) < 2:
            item = '0' + item
        return item

    def get_now(self, item):
        if 'hours' in item:
            delta = datetime.timedelta(hours=1)
            n = int(re.findall(r'\d+', item)[0])
            ddd = datetime.datetime.now() - n * delta
            return '{}-{}-{}'.format(
                self.change(
                    ddd.year), self.change(
                    ddd.month), self.change(
                    ddd.day))
        elif 'minutes' in item:
            delta = datetime.timedelta(minutes=1)
            n = int(re.findall(r'\d+', item)[0])
            ddd = datetime.datetime.now() - n * delta
            return '{}-{}-{}'.format(
                self.change(
                    ddd.year), self.change(
                    ddd.month), self.change(
                    ddd.day))
        elif 'seconds' in item:
            delta = datetime.timedelta(seconds=1)
            n = int(re.findall(r'\d+', item)[0])
            ddd = datetime.datetime.now() - n * delta
            return '{}-{}-{}'.format(
                self.change(
                    ddd.year), self.change(
                    ddd.month), self.change(
                    ddd.day))
        d={'Jan':'01','Feb':'02','Mar':'03','Apr':'04','May':'05','Jun':'06','Jul':'07',
           'Aug':'08','Sep':'09','Oct':10,'Nov':11,'Dec':12}
        day,year=re.findall(r'\d+',item)
        try:
            mon=d[item.split()[-3]]
        except:
            print item
        return '{}-{}-{}'.format(year, self.change_item(mon), self.change_item(day))