"""
This modules implements the CrawlSpider which is the recommended spider to use
for scraping typical web sites that requires crawling pages.

See documentation in docs/topics/spiders.rst
"""

import copy
from typing import Sequence

from scrapy.http import Request, HtmlResponse
from scrapy.linkextractors import LinkExtractor
from scrapy.spiders import Spider
from scrapy.utils.spider import iterate_spider_output


def _identity(x):
    return x


def _identity_process_request(request, response):
    return request


def _get_method(method, spider): #返回该方法 或者spider内的这名字的方法
    if callable(method):
        return method
    elif isinstance(method, str):
        return getattr(spider, method, None)


_default_link_extractor = LinkExtractor()


class Rule:

    def __init__(
        self,
        link_extractor=None,
        callback=None,
        cb_kwargs=None,
        follow=None,
        process_links=None,
        process_request=None,
        errback=None,
    ):
        self.link_extractor = link_extractor or _default_link_extractor #这个用法挺厉害哈
        self.callback = callback #当 link_extractor 返回一个linkurl时候 这个方法会被调用 （参数是response）【返回item或者request的列表】
        self.errback = errback
        self.cb_kwargs = cb_kwargs or {} #这个是callback的 参数（命名参数）
        self.process_links = process_links or _identity #这里指的是 如果传入函数 就用那个函数 不然用 默认的  是一个callable或string(该spider中同名的函数将会被调用)。 从link_extractor中获取到链接列表时将会调用该函数。该方法主要用来过滤。
        self.process_request = process_request or _identity_process_request #同上
        self.follow = follow if follow is not None else not callback #从这个方法返回的链接是否需要跟进（自身）如果没有callback 则为True 否则是False

    def _compile(self, spider): #相当于从spider 中拿到对应callback
        self.callback = _get_method(self.callback, spider)
        self.errback = _get_method(self.errback, spider)
        self.process_links = _get_method(self.process_links, spider)
        self.process_request = _get_method(self.process_request, spider)


class CrawlSpider(Spider):

    rules: Sequence[Rule] = () #限制其rules 属性必须是Rule型的list或者set

    def __init__(self, *a, **kw):
        super().__init__(*a, **kw)
        self._compile_rules() #将rules 对象拷贝 并从spider中拿出来对应的callback 赋到对应的rule对象上

    def _parse(self, response, **kwargs): #系统内部调用_parse_response，传入parse_start_url 开始爬取处
        return self._parse_response(
            response=response,
            callback=self.parse_start_url,
            cb_kwargs=kwargs,
            follow=True,
        )

    def parse_start_url(self, response, **kwargs): #由子类实现，返回一个url列表用于初始爬取
        return []

    def process_results(self, response, results):
        return results

    def _build_request(self, rule_index, link): #构建request
        return Request(
            url=link.url,
            callback=self._callback,
            errback=self._errback,
            meta=dict(rule=rule_index, link_text=link.text),
        )

    def _requests_to_follow(self, response):
        if not isinstance(response, HtmlResponse):
            return
        seen = set()
        for rule_index, rule in enumerate(self._rules):
            links = [lnk for lnk in rule.link_extractor.extract_links(response)
                     if lnk not in seen]
            for link in rule.process_links(links):
                seen.add(link)
                request = self._build_request(rule_index, link)
                yield rule.process_request(request, response)

    def _callback(self, response):
        rule = self._rules[response.meta['rule']]#用meta传递 rule 的key 然后实例化rule
        return self._parse_response(response, rule.callback, rule.cb_kwargs, rule.follow) #用rule做再提 传递 各种callback 和标志位

    def _errback(self, failure): #完全同上
        rule = self._rules[failure.request.meta['rule']]
        return self._handle_failure(failure, rule.errback)

    def _parse_response(self, response, callback, cb_kwargs, follow=True):
        if callback:
            cb_res = callback(response, **cb_kwargs) or ()
            cb_res = self.process_results(response, cb_res)
            for request_or_item in iterate_spider_output(cb_res):
                yield request_or_item

        if follow and self._follow_links:
            for request_or_item in self._requests_to_follow(response):
                yield request_or_item

    def _handle_failure(self, failure, errback):
        if errback:
            results = errback(failure) or ()
            for request_or_item in iterate_spider_output(results):
                yield request_or_item

    def _compile_rules(self):
        self._rules = []
        for rule in self.rules:
            self._rules.append(copy.copy(rule))
            self._rules[-1]._compile(self)

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super().from_crawler(crawler, *args, **kwargs)
        spider._follow_links = crawler.settings.getbool('CRAWLSPIDER_FOLLOW_LINKS', True)
        return spider
