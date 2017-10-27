# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html

from . import settings
import pymysql
import pymongo
import logging

class AnnouncementDistributePipeline(object):
    def process_item(self, item, spider):
        return item


class MysqlPipeline(object):
    def __init__(self):
        self.mysql_host = settings.db_host
        self.mysql_user = settings.db_user
        self.mysql_password = settings.db_pwd
        self.mysql_db = settings.db_dbname
        self.mysql_tb = settings.db_table


    def open_spider(self, spider):
        self.conn = pymysql.connect(
                                    host=self.mysql_host,
                                    user=self.mysql_user,
                                    passwd=self.mysql_password,
                                    db=self.mysql_db,
                                    charset='utf8',
                                    cursorclass=pymysql.cursors.DictCursor
                                    )

    def process_item(self, item, spider):
        cur = self.conn.cursor()
        pdf_id = item['pdf_id']
        stock_code = item['stock_code']
        market = item['market']
        crawl_time = item['crawl_time']
        announce_time = item['announce_time']
        announce_title = item['announce_title']
        pdf_link = item['pdf_link']
        is_tranformed = item['is_tranformed']
        try:
            sql = 'insert into {0} (`pdf_id`,`stock_code`,`market`,`crawl_time`,`announce_time`,`announce_title`,`pdf_link`,`is_tranformed`) values ' \
                  '(%s,%s,%s,%s,%s,%s,%s,%s)'.format(self.mysql_tb)
            cur.execute(sql, (pdf_id, stock_code, market, crawl_time, announce_time, announce_title, pdf_link, is_tranformed))
            logging.info(sql)
            logging.info('++++++++++++insert into success+++++++++++++')
        except Exception as e:
            logging.error('---------insert error--------:{0}'.format(e))
        return item

    def close_spider(self, spider):
        self.conn.commit()
        self.conn.close()


class MongoPipeline(object):
    collection_name = 'users'

    def __init__(self, mongo_uri, mongo_db):
        self.mongo_uri = mongo_uri
        self.mongo_db = mongo_db

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            mongo_uri=crawler.settings.get('MONGO_URI'),
            mongo_db=crawler.settings.get('MONGO_DATABASE')
        )

    def open_spider(self, spider):
        self.client = pymongo.MongoClient(self.mongo_uri)
        self.db = self.client[self.mongo_db]

    def close_spider(self, spider):
        self.client.close()

    def process_item(self, item, spider):
        self.db[self.collection_name].update({'url_token': item['url_token']}, dict(item), True)
        return item