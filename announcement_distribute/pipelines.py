# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html


import pymysql
import pymongo
import logging


class MysqlPipeline(object):
    """insert data to mysql"""

    def __init__(self, db_host, db_user, db_pwd, db_dbname, db_table):
        """init mysql parameter"""

        self.mysql_host = db_host
        self.mysql_user = db_user
        self.mysql_password = db_pwd
        self.mysql_db = db_dbname
        self.mysql_tb = db_table

    @classmethod
    def from_crawler(cls, crawler):
        """get mysql para from settings file """

        return cls(db_host=crawler.settings.get('DB_HOST'),
                   db_user=crawler.settings.get('DB_USER'),
                   db_pwd=crawler.settings.get('DB_PWD'),
                   db_dbname=crawler.settings.get('DB_DBNAME'),
                   db_table=crawler.settings.get('DB_TABLE'))

    def open_spider(self, spider):
        """open spider and connect mysql"""
        self.conn = pymysql.connect(
                                    host=self.mysql_host,
                                    user=self.mysql_user,
                                    passwd=self.mysql_password,
                                    db=self.mysql_db,
                                    charset='utf8',
                                    cursorclass=pymysql.cursors.DictCursor
                                    )

    def process_item(self, item, spider):
        """insert item data to mysql"""

        pdf_id = item['pdf_id']
        stock_code = item['stock_code']
        market = item['market']
        crawl_time = item['crawl_time']
        announce_time = item['announce_time']
        announce_title = item['announce_title']
        pdf_link = item['pdf_link']
        is_tranformed = item['is_tranformed']
        try:
            with self.conn.cursor() as cur:
                sql = 'insert into {0} (`pdf_id`,`stock_code`,`market`,`crawl_time`,`announce_time`,`announce_title`,' \
                      '`pdf_link`,`is_tranformed`) values (%s,%s,%s,%s,%s,%s,%s,%s)'.format(self.mysql_tb)
                cur.execute(sql, (pdf_id, stock_code, market, crawl_time, announce_time, announce_title,
                                  pdf_link, is_tranformed))
                logging.info(sql)
                logging.info('++++++++++++insert into success+++++++++++++')
        except Exception as e:
            logging.error('---------insert error--------:{0}'.format(e))
        return item

    def close_spider(self, spider):
        """commit data to mysql and close connect"""
        try:
            self.conn.commit()
            logging.info('++++++commit success++++')
        except Exception as e:
            logging.error('-----commit faild------')
        finally:
            self.conn.close()


class MongoPipeline(object):
    """you can choose mongodb to save data then you must config the settings file (config ITEM_PIPELINES)"""
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