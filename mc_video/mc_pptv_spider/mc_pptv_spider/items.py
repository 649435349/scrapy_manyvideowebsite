# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html

import scrapy


class McPptvSpiderItem(scrapy.Item):
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
    tag= scrapy.Field()
    introduction= scrapy.Field()

