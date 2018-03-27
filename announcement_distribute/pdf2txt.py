#_*_coding:utf8 _*_

import signal
import os
import logging

from io import StringIO
from pdfminer.pdfinterp import PDFResourceManager, PDFPageInterpreter
from pdfminer.converter import TextConverter
from pdfminer.layout import LAParams
from pdfminer.pdfpage import PDFPage


class TimeOutException(Exception):
    pass

def setTimeout(num):
    """a decorator appoint transform timeout"""
    def wrape(func):
        def handle(signum, frame):
            raise TimeOutException("PDF解析超时！")

        def toDo(*args, **kwargs):
            try:
                signal.signal(signal.SIGALRM, handle)
                signal.alarm(num)
                ret = func(*args, **kwargs)
                signal.alarm(0)
                return ret
            except TimeOutException as e:
                logging.info('pdf parse time out')
                return False

        return toDo
    return wrape

@setTimeout(60)
def pdf2txt(path):
    """pdf transform to txt"""
    ret = 10
    rsrcmgr = PDFResourceManager()
    retstr = StringIO()
    codec = 'utf-8'
    laparams = LAParams()
    fp = open(path, 'rb')
    device = TextConverter(rsrcmgr, retstr, codec=codec, laparams=laparams)
    interpreter = PDFPageInterpreter(rsrcmgr, device)
    password = ""
    maxpages = 0
    caching = True
    pagenos = set()
    for page in PDFPage.get_pages(fp, pagenos, maxpages=maxpages, password=password, caching=caching,
                                  check_extractable=True):
        interpreter.process_page(page)
    text = retstr.getvalue()
    fp.close()
    textFilename = path.replace('PDF', 'txt')
    with open(textFilename, 'w') as f:
        f.write(text)
    if os.path.exists(textFilename):
        with open(textFilename) as f:
            num_lines = sum(1 for line in f)
            if num_lines > 10:
                ret = 11
    return ret