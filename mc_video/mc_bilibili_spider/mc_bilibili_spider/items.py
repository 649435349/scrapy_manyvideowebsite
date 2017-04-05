# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html

import scrapy


class McBilibiliSpiderItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    crawl_time=scrapy.Field()
    video_url=scrapy.Field()
    page_url=scrapy.Field()
    title=scrapy.Field()
    category=scrapy.Field()
    upload_time=scrapy.Field()
    play_cnt=scrapy.Field()
    comment_cnt=scrapy.Field()
    author=scrapy.Field()
    coin_cnt=scrapy.Field()
    collect_cnt=scrapy.Field()
    title_p=scrapy.Field()
    duration=scrapy.Field()
    share_cnt=scrapy.Field()
    label=scrapy.Field()
    introduction=scrapy.Field()
    danmu_cnt=scrapy.Field()
    this_charge_cnt=scrapy.Field()
    rank_top_daily=scrapy.Field()
    personal_signiture=scrapy.Field()
    personal_homepage=scrapy.Field()
    post_cnt=scrapy.Field()
    fans_cnt=scrapy.Field()
    history_charge_cnt=scrapy.Field()

class PublicItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    crawl_time= scrapy.Field()
    website= scrapy.Field()
    video_url= scrapy.Field()
    page_url= scrapy.Field()
    title= scrapy.Field()
    category= scrapy.Field()
    upload_time= scrapy.Field()
    play_cnt= scrapy.Field()
    comment_cnt= scrapy.Field()
    label= scrapy.Field()
    author= scrapy.Field()
    fans_cnt= scrapy.Field()
    post_cnt= scrapy.Field()
    all_play_cnt= scrapy.Field()
    author_url=scrapy.Field()
