# _*_coding:utf8 _*_

import scrapy
import json
import datetime
import os
import copy
import logging
import time
from ..items import AnnouncementDistributeItem
from ..pdf2txt import pdf2txt
from ..settings import PDFPATH

from scrapy.spidermiddlewares.httperror import HttpError
from twisted.internet.error import DNSLookupError
from twisted.internet.error import TimeoutError, TCPTimedOutError

logging.basicConfig(filename='annouce.log', level=logging.ERROR,
                    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")


class announcement(scrapy.Spider):
    name = 'announcement_distribute'

    def start_requests(self):
        url = 'http://www.cninfo.com.cn/cninfo-new/announcement/query?{0}'.format(int(time.time()))
        start_date = '2017-01-01'
        end_date = '2017-09-02'
        data = {
            'plate': 'shmb;sz;szmb;szzx;szcy;',
            'column': 'szse',
            'columnTitle': '历史公告查询',
            'pageNum': '1',
            'pageSize': '30',
            'tabName': 'fulltext',
            'showTitle': 'shmb/plate/沪市主板;sz/plate/深市公司;szmb/plate/深市主板;szzx/plate/中小板;szcy/plate/创业板',
            'seDate': '{0} ~ {1}'.format(start_date, end_date)
        }
        yield scrapy.FormRequest(url=url, formdata=data, meta={'data': data},callback=self.parse)

    def parse(self, response):
        item2 = AnnouncementDistributeItem()
        js = json.loads(response.text)
        conlist = js['announcements']
        AllNum = js['totalRecordNum']
        #path = '/home/he/wudang_share/hezudao/distribute'
        path = PDFPATH
        for i in conlist:
            link = i['adjunctUrl']
            _date = datetime.date.fromtimestamp(int(i['announcementTime'])/1000).strftime('%Y-%m-%d')
            announcementTime = datetime.datetime.fromtimestamp(int(i['announcementTime'])/1000).strftime('%Y-%m-%d %H:%M:%S')
            pdf_id = link.split('/')[-1]
            stock_code = i['secCode']
            if stock_code.startswith('60'):
                item2['pdf_id'] = link.split('/')[-1]
                item2['stock_code'] = i['secCode']+'.SH'
                item2['market'] = 'SH'
                item2['crawl_time'] = datetime.datetime.today().strftime('%Y-%m-%d %H:%M:%S')
                item2['announce_time'] = announcementTime
                item2['announce_title'] = i['announcementTitle']
                item2['pdf_link'] = 'http://www.cninfo.com.cn/'+link
                pdf_path = path + '/' + 'sh' + '/' + _date
                if not os.path.exists(pdf_path):
                    os.makedirs(pdf_path)

            else:
                item2['pdf_id'] = link.split('/')[-1]
                item2['stock_code'] = i['secCode'] + '.SZ'
                item2['market'] = 'SZ'
                item2['crawl_time'] = datetime.datetime.today().strftime('%Y-%m-%d %H:%M:%S')
                item2['announce_time'] = announcementTime
                item2['announce_title'] = i['announcementTitle']
                item2['pdf_link'] = 'http://www.cninfo.com.cn/' + link
                pdf_path = path + '/sz' + '/' + _date
                if not os.path.exists(pdf_path):
                    os.makedirs(pdf_path)

            filename = pdf_path + '/' + pdf_id
            if not os.path.exists(pdf_path):
                os.makedirs(pdf_path)
            if not os.path.isfile(filename):
                request = scrapy.Request(url=item2['pdf_link'], meta={'filename': filename},
                                         errback=self.errback_httpbin, callback=self.download_pdf)
                request.meta['item2'] = copy.deepcopy(item2)
                yield request

        data = response.meta.get('data')
        print data
        pagenum = int(data['pageNum'])
        tag = divmod(int(AllNum), 30)
        if tag[1] != 0:
            totalpage = tag[0] + 1
        else:
            totalpage = tag[0]
        print '***************************************************'
        print pagenum,totalpage
        logging.info('pagenum:{0},totalpage:{1}'.format(pagenum, totalpage))
        print '****************************************************'
        if pagenum < totalpage:
            pagenum = pagenum + 1
            data = data
            data['pageNum'] = str(pagenum)
            url = 'http://www.cninfo.com.cn/cninfo-new/announcement/query?{0}'.format(time.time()*100)
            yield scrapy.FormRequest(url=url, meta={'data': data},
                                     formdata=data, callback=self.parse)

    def download_pdf(self, response):
        logging.info('download pdf')
        item2 = response.meta['item2']
        filename = response.meta.get('filename')
        dir_path = os.path.join(* filename.split('/')[:-1])
        dir_path = dir_path+'/'
        if not os.path.exists(dir_path):
            os.makedirs(dir_path)
        with open(filename, 'w') as f:
            f.write(response.body)
        try:
            item2['is_tranformed'] = pdf2txt(filename)
            logging.info('+++++tranformed++++++ success')
        except Exception as e:
            logging.error('--trandfromed is error {0}'.format(e))
            item2['is_tranformed'] = 10
        yield item2

    def errback_httpbin(self, failure):
        # log all failures
        self.logger.error(repr(failure))
        # in case you want to do something special for some errors,
        # you may need the failure's type:

        if failure.check(HttpError):
            # these exceptions come from HttpError spider middleware
            # you can get the non-200 response
            response = failure.value.response
            self.logger.error('HttpError on %s', response.url)
            item = response.meta['item2']
            item['is_tranformed'] = 10
            yield item

        elif failure.check(DNSLookupError):
            # this is the original request
            request = failure.request
            self.logger.error('DNSLookupError on %s', request.url)

        elif failure.check(TimeoutError, TCPTimedOutError):
            request = failure.request
            self.logger.error('TimeoutError on %s', request.url)