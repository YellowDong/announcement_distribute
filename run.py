#!/usr/bin/env python
# _*_ coding:utf8_*_

from scrapy import cmdline
import datetime


today = datetime.date.today().strftime('%Y-%m-%d')

end_time = today
path = '/home/he/wudang_share/hezudao/distribute'
#path = '/mnt/D_share/hezudao/distribute'

cmdline.execute('scrapy crawl announcement_distribute'.split())