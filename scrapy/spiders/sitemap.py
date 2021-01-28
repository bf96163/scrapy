import re
import logging

from scrapy.spiders import Spider
from scrapy.http import Request, XmlResponse
from scrapy.utils.sitemap import Sitemap, sitemap_urls_from_robots
from scrapy.utils.gz import gunzip, gzip_magic_number


logger = logging.getLogger(__name__)


class SitemapSpider(Spider):

    sitemap_urls = ()#指向您要抓取其网址的站点地图的网址列表。或者指向对应的robots.txt文件 （指向网站地图）
    sitemap_rules = [('', 'parse')] #一个由（正则，回调函数）组成的list，用来对解析和回调函数的判断
    sitemap_follow = [''] #用于sitemap是指向不同sitemap的指针时候 过滤的  一个正则 list
    sitemap_alternate_links = False #当有替代sitemap时候是否抓取（一般指同一个网站不同语言版本）默认False

    def __init__(self, *a, **kw):
        super().__init__(*a, **kw)
        self._cbs = [] #正则和回调函数的列表
        for r, c in self.sitemap_rules:
            if isinstance(c, str):
                c = getattr(self, c) #加载类内部对应的类
            self._cbs.append((regex(r), c))
        self._follow = [regex(x) for x in self.sitemap_follow] #内部用来判断是否要爬的

    def start_requests(self): #同父类
        for url in self.sitemap_urls:
            yield Request(url, self._parse_sitemap)

    def sitemap_filter(self, entries): #用来过滤sitemap本身指向多个其他的sitemap的情况 过滤这些指针
        """This method can be used to filter sitemap entries by their
        attributes, for example, you can filter locs with lastmod greater
        than a given date (see docs).
        """
        for entry in entries:
            yield entry

    def _parse_sitemap(self, response): #从sitemap 到发出第一批request就这这个函数
        if response.url.endswith('/robots.txt'):
            for url in sitemap_urls_from_robots(response.text, base_url=response.url): #这里基本就是逐行解析URL 发出去
                yield Request(url, callback=self._parse_sitemap)
        else:
            body = self._get_sitemap_body(response)
            if body is None:
                logger.warning("Ignoring invalid sitemap: %(response)s",
                               {'response': response}, extra={'spider': self})
                return

            s = Sitemap(body) #用lxml.etree.XMLParser 来解析respond 然后过滤出URL
            it = self.sitemap_filter(s) #用子类的方法再次过滤一次

            if s.type == 'sitemapindex':
                for loc in iterloc(it, self.sitemap_alternate_links):
                    if any(x.search(loc) for x in self._follow):
                        yield Request(loc, callback=self._parse_sitemap)
            elif s.type == 'urlset':
                for loc in iterloc(it, self.sitemap_alternate_links):# 变成生成器
                    for r, c in self._cbs: #判断是否符合 对应的正则规则
                        if r.search(loc):#复合正则规则 抛出request
                            yield Request(loc, callback=c)
                            break

    def _get_sitemap_body(self, response):
        """Return the sitemap body contained in the given response,
        or None if the response is not a sitemap.
        """ #兼容不同格式的sitemap
        if isinstance(response, XmlResponse):
            return response.body
        elif gzip_magic_number(response):
            return gunzip(response.body)
        # actual gzipped sitemap files are decompressed above ;
        # if we are here (response body is not gzipped)
        # and have a response for .xml.gz,
        # it usually means that it was already gunzipped
        # by HttpCompression middleware,
        # the HTTP response being sent with "Content-Encoding: gzip"
        # without actually being a .xml.gz file in the first place,
        # merely XML gzip-compressed on the fly,
        # in other word, here, we have plain XML
        elif response.url.endswith('.xml') or response.url.endswith('.xml.gz'):
            return response.body


def regex(x):
    if isinstance(x, str):
        return re.compile(x)
    return x


def iterloc(it, alt=False):
    for d in it:
        yield d['loc']

        # Also consider alternate URLs (xhtml:link rel="alternate")
        if alt and 'alternate' in d:
            yield from d['alternate']
