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
from mc_tudou_spider.items import McTudouSpiderItem,PublicItem
import traceback
import os
import MySQLdb

if "fengyufei" in ''.join(os.uname()):
    DB_URL = "127.0.0.1"
    DB_USER = "root"
    DB_PSW = "fyf!!961004"
    DB_NAME = "mc_video"
    DB_CHARSET = "utf8"

class mc_tudou_spider(scrapy.Spider):
    name = 'mc_tudou_video_information_spider'
    allowed_domains = ["tudou.com"]
    gets = ''

    def __init__(self, choice=None):
        super(mc_tudou_spider, self).__init__()
        self.choice = choice
        self.conn = MySQLdb.connect(host=DB_URL, user=DB_USER, passwd=DB_PSW, db=DB_NAME, charset=DB_CHARSET)
        self.cur = self.conn.cursor()

    def start_requests(self):
        if self.choice == 'create':
            # 土豆最多100页
            urls = [
                'http://www.soku.com/t/nisearch/%E6%88%91%E7%9A%84%E4%B8%96%E7%95%8C/_cid__time__sort_date_display_album?site=1&limitdate=0&_lg=10&page={}',
                'http://www.soku.com/t/nisearch/%E6%88%91%E7%9A%84%E4%B8%96%E7%95%8C/_cid__time__sort_play_display_album?site=1&limitdate=0&_lg=10&page={}',
                'http://www.soku.com/t/nisearch/minecraft/_cid__time__sort_date_display_album?site=1&limitdate=0&_lg=10&page={}',
                'http://www.soku.com/t/nisearch/minecraft/_cid__time__sort_play_display_album?site=1&limitdate=0&_lg=10&page={}']
            for i in range(1, 11):
                for j in urls:
                    yield Request(j.format(i), callback=self.get_each_video)
        elif self.choice=='update':
            sql='select distinct page_url,upload_time from src_minecraft_public_video_day where website=()'
            self.cur.execute(sql,('tudou'))
            self.gets=self.cur.fetchall()
            for i in self.gets():
                idd=re.findall(r'(?<=view/)[^/]+',i[0])[0]
                yield Request(i[0], callback=self.get_video_information,meta={'idd':idd,'upload_time':i[1]})
        else:
            print 'sth wrong.'


    def get_each_video(self,response):
        for i in response.xpath('//div[@class="v"]'):
            a=i.xpath('.//div[@class="v-meta-title"]/a[1]')
            idd=a.xpath('@_log_vid').extract()[0]
            url='http:'+a.xpath('@href').extract()[0]
            upload_time=self.get_now(i.xpath('.//span[@class="r"]').extract()[0])
            yield  Request(url,callback=self.get_video_information,meta={'idd':idd,'upload_time':upload_time})

    def get_video_information(self,response):
        item=McTudouSpiderItem()
        item['crawl_time'] = time.strftime(
            '%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
        item['page_url']=response.url
        item['upload_time']=response.meta['upload_time']
        item['video_url']='http://www.tudou.com/v/{}/&resourceId=0_04_05_99/v.swf'.format(response.meta['idd'])
        item['title']=response.xpath('//meta[@name="irTitle"]/@content').extract()[0]
        item['category'] = response.xpath('//meta[@name="irCategory"]/@content').extract()[0]
        item['introduction']=self.check_null(response.xpath('//p[@class="v_desc"]/text()').extract())
        item['tag']=' '.join([i.xpath('.//text()').extract()[0] for i in response.xpath('//span[@class="tag_wrap"]/a')])
        t=response.xpath('//script').extract()[1]
        if 'albumplay' not in response.url:
            item['personal_homepage'] = response.xpath('//a[@class="name"]/@href').extract()[0]
            item['author']=response.xpath('//a[@class="name"]/@title').extract()[0]
        else:
            item['personal_homepage'] = response.xpath('//h2[@class=" notips "]/a[1]/@href').extract()[0]
            item['author']=response.xpath('//h2[@class="notips"]/a[1]/text()').extract()[0]
        if not item['author']:
            item['author'] = re.findall(r'(?<=onic:\s\')[^\']+', t)[0]
        if len(re.findall(r'\d+',re.findall(r'(?<=time:\s\')[^\']+',t)[0]))==2:
            item['duration']=str(int(re.findall(r'\d+',re.findall(r'(?<=time:\s\')[^\']+',t)[0])[0])*60+int(re.findall(r'\d+',re.findall(r'(?<=time:\s\')[^\']+',t)[0])[1]))+'s'
        else:
            item['duration'] = str(int(re.findall(r'\d+', re.findall(r'(?<=time:\s\')[^\']+', t)[0])[0]) * 3600 + int(
                re.findall(r'\d+', re.findall(r'(?<=time:\s\')[^\']+', t)[0])[1])*60+int(
                re.findall(r'\d+', re.findall(r'(?<=time:\s\')[^\']+', t)[0])[2])) + 's'
        ocode=re.findall(r'(?<=ocode:\s\')[^\']+',t)[0]
        iid=re.findall(r'(?<=iid:\s)\d+',t)[0]
        url='http://www.tudou.com/crp/itemSum.action?jsoncallback=1&app=1&showArea=true&iabcdefg={}&uabcdefg=0&juabcdefg=01b8t5bctd104s'.format(iid)
        yield Request(url,callback=self.get_json1,meta={'item':item,'idd':response.meta['idd'],'ocode':ocode})

    def get_json1(self,response):
        item=response.meta['item']
        d=json.loads(response.body[2:-1])
        item['comment_cnt']=d['commentNum']
        item['play_cnt']=d['playNum']
        item['up_cnt']=d['digNum']
        url='http://www.tudou.com/uis/userCard.action?jsoncallback=1&app=1&uidCode={}'.format(response.meta['ocode'])
        yield Request(url,callback=self.get_json2,meta={'item':item,'idd':response.meta['idd'],'uid':response.meta['ocode']},dont_filter=True)

    def get_json2(self,response):
        item = response.meta['item']
        d = json.loads(response.body[2:-1])
        item['post_cnt']=d['data']['itemNum']
        item['follower_cnt'] = d['data']['subNum']
        item['fans_cnt'] = d['data']['subedNum']
        yield item
        publicItem = PublicItem()
        publicItem['crawl_time'] = item['crawl_time']
        publicItem['website'] = 'tudou'
        publicItem['video_url'] = item['video_url']
        publicItem['page_url'] = item['page_url']
        publicItem['title'] = item['title']
        publicItem['category'] = item['category']
        publicItem['upload_time'] = item['upload_time']
        publicItem['play_cnt'] = item['play_cnt']
        publicItem['comment_cnt'] = item['comment_cnt']
        publicItem['label'] = item['tag']
        publicItem['author'] = item['author']
        publicItem['fans_cnt'] = item['fans_cnt']
        publicItem['post_cnt'] = item['post_cnt']
        publicItem['all_play_cnt']=d['data']['totalPlayTimes']
        yield publicItem
        url='http://v.tudou.com/item/list.do?uid={}&page={}&pageSize=40&sort=1'
        for i in range(1,11):
            yield Request(url.format(d['data']['uid'],i),callback=self.get_author_video)

    def get_author_video(self,response):
        d=json.loads(response.body)
        for i in d['data']['data']:
            upload_time=i['formatPubDate']
            url='http://www.tudou.com/programs/view/{}/'.format(i['code'])
            yield Request(url,callback=self.get_video_information,meta={'idd':i['code'],'upload_time':upload_time},)


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

    def check_null(self,item):
        if len(item)==0:
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


