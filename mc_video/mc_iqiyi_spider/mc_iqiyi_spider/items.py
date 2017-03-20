# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html

import scrapy


class McIqiyiSpiderItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    crawl_time= scrapy.Field()
    video_url= scrapy.Field()
    page_url= scrapy.Field()
    title= scrapy.Field()
    category= scrapy.Field()
    upload_time= scrapy.Field()
    play_cnt= scrapy.Field()
    comment_cnt= scrapy.Field()
    author= scrapy.Field()
    duration= scrapy.Field()
    topic= scrapy.Field()
    tag= scrapy.Field()
    introduction= scrapy.Field()
    up_cnt= scrapy.Field()
    vote_cnt= scrapy.Field()
    personal_homepage= scrapy.Field()
    personal_signiture= scrapy.Field()
    reward_cnt= scrapy.Field()

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