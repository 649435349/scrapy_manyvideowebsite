# coding:utf-8

from scrapy import cmdline
cmdline.execute("scrapy crawl mc_acfun_video_information_spider -a choice=update".split())