# coding:utf-8

from scrapy import cmdline
cmdline.execute("scrapy crawl mc_bilibili_video_information_spider -a choice=update".split())