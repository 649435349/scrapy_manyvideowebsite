# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html

import scrapy


class McMcbbsSpiderItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    crawl_time = scrapy.Field()
    website = scrapy.Field()
    category = scrapy.Field()
    name = scrapy.Field()
    version = scrapy.Field()
    page_url = scrapy.Field()
    title = scrapy.Field()
    upload_time = scrapy.Field()
    download_cnt = scrapy.Field()
    comment_cnt = scrapy.Field()
    author = scrapy.Field()
    view_cnt = scrapy.Field()
    more = scrapy.Field()
