# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html

import scrapy


class AnnouncementDistributeItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    pdf_id = scrapy.Field()
    stock_code = scrapy.Field()
    market = scrapy.Field()
    crawl_time = scrapy.Field()
    announce_time = scrapy.Field()
    announce_title = scrapy.Field()
    pdf_link = scrapy.Field()
    is_tranformed = scrapy.Field()
