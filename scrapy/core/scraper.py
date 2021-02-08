"""This module implements the Scraper component which parses responses and
extracts information from them"""

import logging
from collections import deque

from itemadapter import is_item
from twisted.internet import defer
from twisted.python.failure import Failure

from scrapy import signals
from scrapy.core.spidermw import SpiderMiddlewareManager
from scrapy.exceptions import CloseSpider, DropItem, IgnoreRequest
from scrapy.http import Request, Response
from scrapy.utils.defer import defer_fail, defer_succeed, iter_errback, parallel
from scrapy.utils.log import failure_to_exc_info, logformatter_adapter
from scrapy.utils.misc import load_object, warn_on_generator_with_return_value
from scrapy.utils.spider import iterate_spider_output


logger = logging.getLogger(__name__)

# 内部数据处理类
class Slot:
    """Scraper slot (one per running spider)"""

    MIN_RESPONSE_SIZE = 1024

    def __init__(self, max_active_size=5000000):
        self.max_active_size = max_active_size
        self.queue = deque()
        self.active = set()
        self.active_size = 0
        self.itemproc_size = 0
        self.closing = None
    # 将respond 以(response, request, deferred) 格式压入self.queue
    def add_response_request(self, response, request):
        deferred = defer.Deferred()
        self.queue.append((response, request, deferred))
        if isinstance(response, Response):
            self.active_size += max(len(response.body), self.MIN_RESPONSE_SIZE)
        else:
            self.active_size += self.MIN_RESPONSE_SIZE
        return deferred
    # 取出(response, request, deferred) 并将 request 加入到 self.active
    def next_response_request_deferred(self):
        response, request, deferred = self.queue.popleft()
        self.active.add(request)
        return response, request, deferred
    # 从self.active里移出 request 从 self.active_size中移出对应size
    def finish_response(self, response, request):
        self.active.remove(request)
        if isinstance(response, Response):
            self.active_size -= max(len(response.body), self.MIN_RESPONSE_SIZE)
        else:
            self.active_size -= self.MIN_RESPONSE_SIZE
    # que 和 active 都为空 就是闲着
    def is_idle(self):
        return not (self.queue or self.active)
    # 当内部存的值已经大于定义的最大值
    def needs_backout(self):
        return self.active_size > self.max_active_size


class Scraper:
    # 在engine初始化的时候 就将这个scraper初始化
    def __init__(self, crawler):
        self.slot = None #不同于 engine里的slot 这里主要处理返回的respond 和 request
        self.spidermw = SpiderMiddlewareManager.from_crawler(crawler)
        itemproc_cls = load_object(crawler.settings['ITEM_PROCESSOR'])
        self.itemproc = itemproc_cls.from_crawler(crawler) # 生成 ITEM_PROCESSOR 类实例
        self.concurrent_items = crawler.settings.getint('CONCURRENT_ITEMS') # 同时处理item个数
        self.crawler = crawler
        self.signals = crawler.signals
        self.logformatter = crawler.logformatter
    # 在engine.open_spider阶段 会调起这个方法 作为最后的准备工作
    @defer.inlineCallbacks
    def open_spider(self, spider):
        """Open the given spider for scraping and allocate resources for it"""
        self.slot = Slot(self.crawler.settings.getint('SCRAPER_SLOT_MAX_ACTIVE_SIZE')) #从setting中 拿到slot最大处理空间设置（缓存设置）
        yield self.itemproc.open_spider(spider) #调起 处理item的 .open_spider方法

    def close_spider(self, spider):
        """Close a spider being scraped and release its resources"""
        slot = self.slot
        slot.closing = defer.Deferred()
        slot.closing.addCallback(self.itemproc.close_spider)
        self._check_if_closing(spider, slot)
        return slot.closing

    def is_idle(self):
        """Return True if there isn't any more spiders to process"""
        return not self.slot

    def _check_if_closing(self, spider, slot):
        if slot.closing and slot.is_idle():
            slot.closing.callback(spider)
    ###### response 实际的处理入口 也就是数据的实际返回后处理的位置 在 engine._handle_downloader_output 被 engine._next_request_from_scheduler 这块加入到request的回调链路里
    def enqueue_scrape(self, response, request, spider): # 这里的response 实际上是一个deferred对象
        slot = self.slot
        dfd = slot.add_response_request(response, request) # 将数据压入缓存

        def finish_scraping(_):
            slot.finish_response(response, request) #从slot 移出这个结果
            self._check_if_closing(spider, slot) # 检查自身是否处于正在关闭状态
            self._scrape_next(spider, slot) #注册当这个respond 处理完后 处理下一个 （仅仅是在deferred处理链路上注册这个工作）
            return _

        dfd.addBoth(finish_scraping)
        dfd.addErrback(
            lambda f: logger.error('Scraper bug processing %(request)s',
                                   {'request': request},
                                   exc_info=failure_to_exc_info(f),
                                   extra={'spider': spider}))
        self._scrape_next(spider, slot) ######### 这里才是调用slot执行工作 （注意是执行slot里的工作 并非对应的request ）
        return dfd
    # 从slot里取出压入的数据 并调用_scrape
    def _scrape_next(self, spider, slot): #弹出一组 respond 和request
        while slot.queue:
            response, request, deferred = slot.next_response_request_deferred()
            self._scrape(response, request, spider).chainDeferred(deferred)# 这里的意思 是先调用 _scrape 然后 执行 defferred的操作
    # 判断返回类型是否是 Response者Failure 调用_scrape2 添加 对应解析的 callback 和errback
    def _scrape(self, result, request, spider):
        """
        Handle the downloaded response or failure through the spider callback/errback
        """
        if not isinstance(result, (Response, Failure)):
            raise TypeError(f"Incorrect type: expected Response or Failure, got {type(result)}: {result!r}")
        dfd = self._scrape2(result, request, spider)  # returns spider's processed output
        dfd.addErrback(self.handle_spider_error, request, result, spider) #添加 errback
        dfd.addCallback(self.handle_spider_output, request, result, spider)# 添加 callback
        return dfd

    def _scrape2(self, result, request, spider):
        """
        Handle the different cases of request's result been a Response or a Failure
        """
        if isinstance(result, Response):
            return self.spidermw.scrape_response(self.call_spider, result, request, spider) # 调用 sipider middle ware 来处理respond 返回deferred
        else:  # result is a Failure
            dfd = self.call_spider(result, request, spider) # 对deferred 进行操作
            return dfd.addErrback(self._log_download_errors, result, request, spider)
    # 给对应的 result类型添加对应的 callback 或者 errback
    def call_spider(self, result, request, spider):
        if isinstance(result, Response): #从spider中拿到的事Response对象
            if getattr(result, "request", None) is None:
                result.request = request
            callback = result.request.callback or spider._parse # 从request对象里面拿到 对应的callback 否则传入spider的_parse函数作为callback
            warn_on_generator_with_return_value(spider, callback)
            dfd = defer_succeed(result)
            dfd.addCallback(callback, **result.request.cb_kwargs)# 将spider的callback 添加到 deferred的回调链路上
        else:  # result is a Failure
            result.request = request
            warn_on_generator_with_return_value(spider, request.errback)
            dfd = defer_fail(result)
            dfd.addErrback(request.errback)
        return dfd.addCallback(iterate_spider_output)
    # _failure 是deferred 对象 失败时候后返回的对象 负责通知engine close_spider 或者log错误
    def handle_spider_error(self, _failure, request, response, spider):
        exc = _failure.value
        if isinstance(exc, CloseSpider):
            self.crawler.engine.close_spider(spider, exc.reason or 'cancelled') # 通知 engine 执行对应 close_spider对象
            return
        logkws = self.logformatter.spider_error(_failure, request, response, spider)
        logger.log(
            *logformatter_adapter(logkws),
            exc_info=failure_to_exc_info(_failure),
            extra={'spider': spider}
        )
        self.signals.send_catch_log(
            signal=signals.spider_error,
            failure=_failure, response=response,
            spider=spider
        )
        self.crawler.stats.inc_value(
            f"spider_exceptions/{_failure.value.__class__.__name__}",
            spider=spider
        )

    def handle_spider_output(self, result, request, response, spider): #多线程处理 item
        if not result:
            return defer_succeed(None)# 清空deferred
        it = iter_errback(result, self.handle_spider_error, request, response, spider)
        dfd = parallel(it, self.concurrent_items, self._process_spidermw_output,
                       request, response, spider)# 并行处理 self._process_spidermw_output
        return dfd

    def _process_spidermw_output(self, output, request, response, spider):
        """Process each Request/Item (given in the output parameter) returned
        from the given spider
        """
        if isinstance(output, Request):
            self.crawler.engine.crawl(request=output, spider=spider) #丢给 engine 处理
        elif is_item(output):
            self.slot.itemproc_size += 1 # slot 正在处理 item计数器+1
            dfd = self.itemproc.process_item(output, spider) #用处理ItemPipelineManager的类 output（item）
            dfd.addBoth(self._itemproc_finished, output, response, spider) # item回调链 添加self._itemproc_finished
            return dfd
        elif output is None:
            pass
        else:
            typename = type(output).__name__
            logger.error(
                'Spider must return request, item, or None, got %(typename)r in %(request)s',
                {'request': request, 'typename': typename},
                extra={'spider': spider},
            )

    def _log_download_errors(self, spider_failure, download_failure, request, spider):
        """Log and silence errors that come from the engine (typically download
        errors that got propagated thru here)
        """
        if isinstance(download_failure, Failure) and not download_failure.check(IgnoreRequest):
            if download_failure.frames:
                logkws = self.logformatter.download_error(download_failure, request, spider)
                logger.log(
                    *logformatter_adapter(logkws),
                    extra={'spider': spider},
                    exc_info=failure_to_exc_info(download_failure),
                )
            else:
                errmsg = download_failure.getErrorMessage()
                if errmsg:
                    logkws = self.logformatter.download_error(
                        download_failure, request, spider, errmsg)
                    logger.log(
                        *logformatter_adapter(logkws),
                        extra={'spider': spider},
                    )

        if spider_failure is not download_failure:
            return spider_failure

    def _itemproc_finished(self, output, item, response, spider):
        """ItemProcessor finished for the given ``item`` and returned ``output``
        """
        self.slot.itemproc_size -= 1 #计数器减1
        if isinstance(output, Failure): # 处理item处理错误
            ex = output.value
            if isinstance(ex, DropItem):
                logkws = self.logformatter.dropped(item, ex, response, spider)
                if logkws is not None:
                    logger.log(*logformatter_adapter(logkws), extra={'spider': spider})
                return self.signals.send_catch_log_deferred(
                    signal=signals.item_dropped, item=item, response=response,
                    spider=spider, exception=output.value)
            else:
                logkws = self.logformatter.item_error(item, ex, response, spider)
                logger.log(*logformatter_adapter(logkws), extra={'spider': spider},
                           exc_info=failure_to_exc_info(output))
                return self.signals.send_catch_log_deferred(
                    signal=signals.item_error, item=item, response=response,
                    spider=spider, failure=output)
        else:
            logkws = self.logformatter.scraped(output, response, spider)
            if logkws is not None:
                logger.log(*logformatter_adapter(logkws), extra={'spider': spider})
            return self.signals.send_catch_log_deferred(
                signal=signals.item_scraped, item=output, response=response,
                spider=spider)
