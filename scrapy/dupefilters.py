import os
import logging

from scrapy.utils.job import job_dir
from scrapy.utils.request import referer_str, request_fingerprint


class BaseDupeFilter:

    @classmethod
    def from_settings(cls, settings):
        return cls()

    def request_seen(self, request):
        return False

    def open(self):  # can return deferred
        pass

    def close(self, reason):  # can return a deferred
        pass

    def log(self, request, spider):  # log that a request has been filtered
        pass


class RFPDupeFilter(BaseDupeFilter):
    """Request Fingerprint duplicates filter"""

    def __init__(self, path=None, debug=False):
        self.file = None
        self.fingerprints = set()
        self.logdupes = True
        self.debug = debug
        self.logger = logging.getLogger(__name__)
        # 如果指定path 将self.path 指向对应path目录下的 requests.seen
        if path:
            self.file = open(os.path.join(path, 'requests.seen'), 'a+')
            self.file.seek(0) #将指针设置到文件头
            self.fingerprints.update(x.rstrip() for x in self.file) #从文件中拿到指纹

    @classmethod
    def from_settings(cls, settings):
        debug = settings.getbool('DUPEFILTER_DEBUG')
        return cls(job_dir(settings), debug)

    # 简单理解 指纹在self.fingerprints就返回true 否则将指纹加入
    def request_seen(self, request):
        fp = self.request_fingerprint(request)
        if fp in self.fingerprints:
            return True
        self.fingerprints.add(fp)
        if self.file:
            self.file.write(fp + '\n')

    def request_fingerprint(self, request):
        # request_fingerprint可选参数有 include_headers=None, keep_fragments=False
        #  include_headers 如果哪一个header不同则认定是不同request时候可添加这一个项目（传入可迭代对象）
        #  keep_fragments 是通过URL传入的参数是否作为区分不同request的区别
        return request_fingerprint(request)

    def close(self, reason):
        if self.file:
            self.file.close()

    def log(self, request, spider):
        if self.debug:
            msg = "Filtered duplicate request: %(request)s (referer: %(referer)s)"
            args = {'request': request, 'referer': referer_str(request)}
            self.logger.debug(msg, args, extra={'spider': spider})
        elif self.logdupes:
            msg = ("Filtered duplicate request: %(request)s"
                   " - no more duplicates will be shown"
                   " (see DUPEFILTER_DEBUG to show all duplicates)")
            self.logger.debug(msg, {'request': request}, extra={'spider': spider})
            self.logdupes = False
        # 给spider的统计 项目 增加数据
        spider.crawler.stats.inc_value('dupefilter/filtered', spider=spider)
