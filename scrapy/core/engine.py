"""
This is the Scrapy engine which controls the Scheduler, Downloader and Spiders.

For more information see docs/topics/architecture.rst

"""
import logging
from time import time

from twisted.internet import defer, task
from twisted.python.failure import Failure

from scrapy import signals
from scrapy.core.scraper import Scraper
from scrapy.exceptions import DontCloseSpider
from scrapy.http import Response, Request
from scrapy.utils.misc import load_object
from scrapy.utils.reactor import CallLaterOnce
from scrapy.utils.log import logformatter_adapter, failure_to_exc_info

logger = logging.getLogger(__name__)


class Slot:

    def __init__(self, start_requests, close_if_idle, nextcall, scheduler):
        self.closing = False
        self.inprogress = set()  # requests in progress
        self.start_requests = iter(start_requests) #变成迭代器
        self.close_if_idle = close_if_idle
        self.nextcall = nextcall
        self.scheduler = scheduler
        self.heartbeat = task.LoopingCall(nextcall.schedule)

    def add_request(self, request):
        self.inprogress.add(request)

    def remove_request(self, request):
        self.inprogress.remove(request)
        self._maybe_fire_closing()

    def close(self):
        self.closing = defer.Deferred()
        self._maybe_fire_closing()
        return self.closing

    def _maybe_fire_closing(self):
        if self.closing and not self.inprogress:
            if self.nextcall:
                self.nextcall.cancel()
                if self.heartbeat.running:
                    self.heartbeat.stop()
            self.closing.callback(None)


class ExecutionEngine:

    def __init__(self, crawler, spider_closed_callback):
        self.crawler = crawler
        self.settings = crawler.settings
        self.signals = crawler.signals #当 crawler 初始化应该初始化了一个 signalmanager 它里面的 sender 就是 crawler
        self.logformatter = crawler.logformatter
        self.slot = None
        self.spider = None
        self.running = False
        self.paused = False
        self.scheduler_cls = load_object(self.settings['SCHEDULER'])
        downloader_cls = load_object(self.settings['DOWNLOADER'])
        self.downloader = downloader_cls(crawler)
        self.scraper = Scraper(crawler)
        self._spider_closed_callback = spider_closed_callback
    # 这里只是设置开启标志 并返回deferred对象 真正的准备阶段在 open_spider
    @defer.inlineCallbacks
    def start(self):
        """Start the execution engine"""
        if self.running:
            raise RuntimeError("Engine already running")
        self.start_time = time()
        yield self.signals.send_catch_log_deferred(signal=signals.engine_started) #增加 errback  并发送一次 signal
        self.running = True
        self._closewait = defer.Deferred()
        yield self._closewait

    def stop(self):
        """Stop the execution engine gracefully"""
        if not self.running:
            raise RuntimeError("Engine not running")
        self.running = False #打标记
        dfd = self._close_all_spiders() #拿到关闭deferred
        return dfd.addBoth(lambda _: self._finish_stopping_engine()) #给deferred添加 关闭的callback

    def close(self):
        """Close the execution engine gracefully.

        If it has already been started, stop it. In all cases, close all spiders
        and the downloader.
        """
        if self.running:
            # Will also close spiders and downloader
            return self.stop()
        elif self.open_spiders:
            # Will also close downloader
            return self._close_all_spiders()
        else:
            return defer.succeed(self.downloader.close()) #其实就是相当于d.callback(result)

    def pause(self):
        """Pause the execution engine"""
        self.paused = True

    def unpause(self):
        """Resume the execution engine"""
        self.paused = False
    # 判断暂停否，取出一个request  调用crawl 处理request 最后判断是否空闲关闭
    def _next_request(self, spider):
        slot = self.slot
        if not slot:
            return

        if self.paused:
            return

        while not self._needs_backout(spider):
            if not self._next_request_from_scheduler(spider): #如果从scheduler 取出 request 并添加 item处理的callback 查看其返回结果 没有了就break
                break

        if slot.start_requests and not self._needs_backout(spider): #处理关闭后仍有request情况
            try:
                request = next(slot.start_requests)
            except StopIteration:
                slot.start_requests = None
            except Exception:
                slot.start_requests = None
                logger.error('Error while obtaining start requests',
                             exc_info=True, extra={'spider': spider})
            else:
                self.crawl(request, spider)# 调用自身crawl方法爬

        if self.spider_is_idle(spider) and slot.close_if_idle: #空闲时候是否关闭spider
            self._spider_idle(spider)
    # 从不同地方的标志位置判断是否需要退出
    def _needs_backout(self, spider):
        slot = self.slot
        return (
            not self.running
            or slot.closing
            or self.downloader.needs_backout()
            or self.scraper.slot.needs_backout()
        )
    # 从scheduler中取出request，放到_download下载 再给他添加处理返回值 和 调用下一个循环的回调
    def _next_request_from_scheduler(self, spider):
        slot = self.slot
        request = slot.scheduler.next_request() # 从scheduler中取出request
        if not request:
            return
        d = self._download(request, spider) #将request放到_download中 生成deferred
        d.addBoth(self._handle_downloader_output, request, spider) # #### 这里其实就是 处理返回request的 回调函数 其中 函数的 response 就是这个deferred对象
        d.addErrback(lambda f: logger.info('Error while handling downloader output',
                                           exc_info=failure_to_exc_info(f),
                                           extra={'spider': spider}))
        d.addBoth(lambda _: slot.remove_request(request)) #从slot中删掉对应的reqeust
        d.addErrback(lambda f: logger.info('Error while removing request from slot',
                                           exc_info=failure_to_exc_info(f),
                                           extra={'spider': spider}))
        d.addBoth(lambda _: slot.nextcall.schedule()) # 添加一个运行下一个request的回调
        d.addErrback(lambda f: logger.info('Error while scheduling new request',
                                           exc_info=failure_to_exc_info(f),
                                           extra={'spider': spider}))
        return d
    # 判断resonse具体类别 正确情况下调用scraper.enqueue_scrape 压入响应并返回deferred对象，errback 添加log
    def _handle_downloader_output(self, response, request, spider):
        if not isinstance(response, (Request, Response, Failure)):
            raise TypeError(
                "Incorrect type: expected Request, Response or Failure, got "
                f"{type(response)}: {response!r}"
            )
        # downloader middleware can return requests (for example, redirects)
        if isinstance(response, Request):
            self.crawl(response, spider) #将request 压入 scheduler
            return
        # response is a Response or Failure
        d = self.scraper.enqueue_scrape(response, request, spider) #将 request respond 和sipider 共同的做作用好的scraper 返回来
        d.addErrback(lambda f: logger.error('Error while enqueuing downloader output',
                                            exc_info=failure_to_exc_info(f),
                                            extra={'spider': spider}))
        return d #返回deferred
    #判断spider是不是闲着
    def spider_is_idle(self, spider): #判断 scrapy整体是不是 闲着
        if not self.scraper.slot.is_idle():
            # scraper is not idle
            return False

        if self.downloader.active:
            # downloader has pending requests
            return False

        if self.slot.start_requests is not None:
            # not all start requests are handled
            return False

        if self.slot.scheduler.has_pending_requests():
            # scheduler has pending requests
            return False

        return True

    @property
    def open_spiders(self): # 目前一个engine还是只能使用一个spider
        return [self.spider] if self.spider else []

    def has_capacity(self): #一个引擎对应一个slot
        """Does the engine have capacity to handle more spiders"""
        return not bool(self.slot)
    #执行单个的 request 压入scheduler 并执行下一步的命令
    def crawl(self, request, spider):
        if spider not in self.open_spiders:
            raise RuntimeError(f"Spider {spider.name!r} not opened when crawling: {request}")
        self.schedule(request, spider) #将request 压入scheduler【压入数据】
        self.slot.nextcall.schedule() #执行下一步操作 这里的nextcall 是调用 _next_request()【取出request并后处理】
    # 将request 压入spider的que中
    def schedule(self, request, spider):
        self.signals.send_catch_log(signals.request_scheduled, request=request, spider=spider)
        if not self.slot.scheduler.enqueue_request(request): #入队列 并陪你段是否被过滤
            self.signals.send_catch_log(signals.request_dropped, request=request, spider=spider)
    # 这里是调用 _downlaod 方法 进行下载 最后添加一个 _downloaded 到回调链路上
    def download(self, request, spider):
        d = self._download(request, spider)
        d.addBoth(self._downloaded, self.slot, request, spider) #addBoth（func， ，参数 ，参数）
        return d
    #是request的话 从新调用 dowload 否则返回 rewponse
    def _downloaded(self, response, slot, request, spider):
        slot.remove_request(request) #从slot里删除这个request
        return self.download(response, spider) if isinstance(response, Request) else response
    #### 下载起点slot ##### 添加这个 request 然后调用downloader 下载 request 添加对应的 处理callback
    def _download(self, request, spider): #
        slot = self.slot
        slot.add_request(request) # 在slot 的正在处理的request里面 增加这个request

        def _on_success(response):
            if not isinstance(response, (Response, Request)):
                raise TypeError(
                    "Incorrect type: expected Response or Request, got "
                    f"{type(response)}: {response!r}"
                )
            if isinstance(response, Response):
                if response.request is None:
                    response.request = request
                logkws = self.logformatter.crawled(response.request, response, spider)
                if logkws is not None:
                    logger.log(*logformatter_adapter(logkws), extra={'spider': spider})
                self.signals.send_catch_log(
                    signal=signals.response_received,
                    response=response,
                    request=response.request,
                    spider=spider,
                )
            return response

        def _on_complete(_):
            slot.nextcall.schedule()
            return _

        dwld = self.downloader.fetch(request, spider) #实际下载发生的情况
        dwld.addCallbacks(_on_success) #增加 callback
        dwld.addBoth(_on_complete) #增加callback 和errback
        return dwld
    ######## 程序的实际开始的地方 ###### 相当于 NO1 实际上一个引擎只能调用一个spider  这个方法中很多spider都只能是个例 不能是list
    @defer.inlineCallbacks
    def open_spider(self, spider, start_requests=(), close_if_idle=True):
        if not self.has_capacity():
            raise RuntimeError(f"No free spider slot when opening {spider.name!r}")
        logger.info("Spider opened", extra={'spider': spider})
        nextcall = CallLaterOnce(self._next_request, spider) # 这里相当于 创建了一个 CallLaterOnce的对象 目标是从start_request里开始抛出request  不过 他是相当于仅仅生成个deferred
        scheduler = self.scheduler_cls.from_crawler(self.crawler)# 实例化scheduler
        start_requests = yield self.scraper.spidermw.process_start_requests(start_requests, spider) # 将request 添加各种callback 和errback
        slot = Slot(start_requests, close_if_idle, nextcall, scheduler) #这里创建对应的slot
        self.slot = slot
        self.spider = spider
        yield scheduler.open(spider) # 初始化 scheduler 生成que
        yield self.scraper.open_spider(spider)# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~·
        self.crawler.stats.open_spider(spider)
        yield self.signals.send_catch_log_deferred(signals.spider_opened, spider=spider)#发出信号
        slot.nextcall.schedule() # 给reactor 添加任务 实际启动_next_request
        slot.heartbeat.start(5) # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~·

    def _spider_idle(self, spider):
        """Called when a spider gets idle. This function is called when there
        are no remaining pages to download or schedule. It can be called
        multiple times. If some extension raises a DontCloseSpider exception
        (in the spider_idle signal handler) the spider is not closed until the
        next loop and this function is guaranteed to be called (at least) once
        again for this spider.
        """
        res = self.signals.send_catch_log(signals.spider_idle, spider=spider, dont_log=DontCloseSpider) #方法返回的类型为 (receiver, result)
        if any(isinstance(x, Failure) and isinstance(x.value, DontCloseSpider) for _, x in res):
            return #当信号发出，返回的是错误 且 不许关闭spider标志的话 返回空

        if self.spider_is_idle(spider):
            self.close_spider(spider, reason='finished')

    def close_spider(self, spider, reason='cancelled'):
        """Close (cancel) spider and clear all its outstanding requests"""

        slot = self.slot
        if slot.closing:
            return slot.closing
        logger.info("Closing spider (%(reason)s)",
                    {'reason': reason},
                    extra={'spider': spider})

        dfd = slot.close()

        def log_failure(msg):
            def errback(failure):
                logger.error(
                    msg,
                    exc_info=failure_to_exc_info(failure),
                    extra={'spider': spider}
                )
            return errback

        dfd.addBoth(lambda _: self.downloader.close()) #通知downloader 关闭动作
        dfd.addErrback(log_failure('Downloader close failure'))

        dfd.addBoth(lambda _: self.scraper.close_spider(spider)) # 通知scraper 关闭动作
        dfd.addErrback(log_failure('Scraper close failure'))

        dfd.addBoth(lambda _: slot.scheduler.close(reason)) #通知slot
        dfd.addErrback(log_failure('Scheduler close failure'))

        dfd.addBoth(lambda _: self.signals.send_catch_log_deferred( #发送信号
            signal=signals.spider_closed, spider=spider, reason=reason))
        dfd.addErrback(log_failure('Error while sending spider_close signal'))

        dfd.addBoth(lambda _: self.crawler.stats.close_spider(spider, reason=reason)) #crawler 关闭信息
        dfd.addErrback(log_failure('Stats close failure'))

        dfd.addBoth(lambda _: logger.info("Spider closed (%(reason)s)",
                                          {'reason': reason},
                                          extra={'spider': spider}))

        dfd.addBoth(lambda _: setattr(self, 'slot', None)) #清理内存
        dfd.addErrback(log_failure('Error while unassigning slot'))

        dfd.addBoth(lambda _: setattr(self, 'spider', None))
        dfd.addErrback(log_failure('Error while unassigning spider'))

        dfd.addBoth(lambda _: self._spider_closed_callback(spider)) #调用对应回调函数

        return dfd

    def _close_all_spiders(self):
        dfds = [self.close_spider(s, reason='shutdown') for s in self.open_spiders]
        dlist = defer.DeferredList(dfds)
        return dlist

    @defer.inlineCallbacks
    def _finish_stopping_engine(self):
        yield self.signals.send_catch_log_deferred(signal=signals.engine_stopped) #
        self._closewait.callback(None)
