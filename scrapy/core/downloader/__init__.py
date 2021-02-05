import random
from time import time
from datetime import datetime
from collections import deque

from twisted.internet import defer, task

from scrapy.utils.defer import mustbe_deferred
from scrapy.utils.httpobj import urlparse_cached
from scrapy.resolver import dnscache
from scrapy import signals
from scrapy.core.downloader.middleware import DownloaderMiddlewareManager
from scrapy.core.downloader.handlers import DownloadHandlers


class Slot:
    """Downloader slot"""
# 控制 同一个IP 或者 同一个域名下 并发和 延时等功能
    def __init__(self, concurrency, delay, randomize_delay):
        self.concurrency = concurrency
        self.delay = delay
        self.randomize_delay = randomize_delay

        self.active = set()
        self.queue = deque()
        self.transferring = set()
        self.lastseen = 0
        self.latercall = None
    # 有空余的 链接
    def free_transfer_slots(self):
        return self.concurrency - len(self.transferring)

    def download_delay(self):#这里的delay是介于 0.5x ~ 1.5x请注意·
        if self.randomize_delay:
            return random.uniform(0.5 * self.delay, 1.5 * self.delay)
        return self.delay

    def close(self):
        if self.latercall and self.latercall.active():
            self.latercall.cancel()

    def __repr__(self):
        cls_name = self.__class__.__name__
        return (f"{cls_name}(concurrency={self.concurrency!r}, "
                f"delay={self.delay:.2f}, "
                f"randomize_delay={self.randomize_delay!r})")

    def __str__(self):
        return (
            f"<downloader.Slot concurrency={self.concurrency!r} "
            f"delay={self.delay:.2f} randomize_delay={self.randomize_delay!r} "
            f"len(active)={len(self.active)} len(queue)={len(self.queue)} "
            f"len(transferring)={len(self.transferring)} "
            f"lastseen={datetime.fromtimestamp(self.lastseen).isoformat()}>"
        )

#从setting和 spider中拿到 downlaod_delay 和 max_concurrent——requests
def _get_concurrency_delay(concurrency, spider, settings):
    delay = settings.getfloat('DOWNLOAD_DELAY')
    if hasattr(spider, 'download_delay'):
        delay = spider.download_delay

    if hasattr(spider, 'max_concurrent_requests'):
        concurrency = spider.max_concurrent_requests

    return concurrency, delay


class Downloader:

    DOWNLOAD_SLOT = 'download_slot'

    def __init__(self, crawler):
        self.settings = crawler.settings
        self.signals = crawler.signals
        self.slots = {}
        self.active = set()
        self.handlers = DownloadHandlers(crawler)
        self.total_concurrency = self.settings.getint('CONCURRENT_REQUESTS')
        self.domain_concurrency = self.settings.getint('CONCURRENT_REQUESTS_PER_DOMAIN')
        self.ip_concurrency = self.settings.getint('CONCURRENT_REQUESTS_PER_IP')
        self.randomize_delay = self.settings.getbool('RANDOMIZE_DOWNLOAD_DELAY')
        self.middleware = DownloaderMiddlewareManager.from_crawler(crawler)
        self._slot_gc_loop = task.LoopingCall(self._slot_gc) #用于检测其内部是否还有东西 没有东西就关闭了
        self._slot_gc_loop.start(60)
    #####实际的下载发生部分##### 由于engine._download 函数调用
    def fetch(self, request, spider):
        def _deactivate(response):
            self.active.remove(request)
            return response

        self.active.add(request)
        dfd = self.middleware.download(self._enqueue_request, request, spider) #如果中间件没有返回response 的话 就调用self._enqueue_request
        return dfd.addBoth(_deactivate)
    #内部激活request大于self.total_concurrency 就标记需要等等
    def needs_backout(self):
        return len(self.active) >= self.total_concurrency
    # 返回对应的KEY和对应slot实例
    def _get_slot(self, request, spider):
        key = self._get_slot_key(request, spider)
        if key not in self.slots:
            conc = self.ip_concurrency if self.ip_concurrency else self.domain_concurrency #ip_concurrency 有限度 高于domain_concurrency
            conc, delay = _get_concurrency_delay(conc, spider, self.settings) #返回的是最大同时处理数 and 延时
            self.slots[key] = Slot(conc, delay, self.randomize_delay)

        return key, self.slots[key]

    def _get_slot_key(self, request, spider):
        if self.DOWNLOAD_SLOT in request.meta: #如果request.meta里有 这个slot信息 就调用那个
            return request.meta[self.DOWNLOAD_SLOT]
        #否则 key是hostname 或者""
        key = urlparse_cached(request).hostname or ''
        if self.ip_concurrency:
            key = dnscache.get(key, key) #从 DNS缓存总拿到对应host的key

        return key
    # 将request 压入slot 并且 从slot中取出 request 进行爬去
    def _enqueue_request(self, request, spider):
        key, slot = self._get_slot(request, spider)
        request.meta[self.DOWNLOAD_SLOT] = key #将KEY压入 request.meta
        # 从对应的slot里
        def _deactivate(response):
            slot.active.remove(request)
            return response

        slot.active.add(request) #添加到激活李彪里
        self.signals.send_catch_log(signal=signals.request_reached_downloader,
                                    request=request,
                                    spider=spider) #发信号signals.request_reached_downloader
        deferred = defer.Deferred().addBoth(_deactivate) #回调练添加 对应方法
        slot.queue.append((request, deferred))
        self._process_queue(spider, slot) ############ 这里就是调用方法 从slot中取出对应的
        return deferred
    #### 实际取出request 并且进行爬去的
    def _process_queue(self, spider, slot):
        from twisted.internet import reactor
        if slot.latercall and slot.latercall.active(): #这里是放到了reactor 后开始跑了
            return

        # Delay queue processing if a download_delay is configured 这里是没放到 reator中 放进去
        now = time()
        delay = slot.download_delay()
        if delay:
            penalty = delay - now + slot.lastseen
            if penalty > 0:
                slot.latercall = reactor.callLater(penalty, self._process_queue, spider, slot)
                return
            #计算延时符合后 运行下面的代码
        # Process enqueued requests if there are free slots to transfer for this slot
        while slot.queue and slot.free_transfer_slots() > 0:
            slot.lastseen = now
            request, deferred = slot.queue.popleft()
            dfd = self._download(slot, request, spider)
            dfd.chainDeferred(deferred)
            # prevent burst if inter-request delays were configured
            if delay:
                self._process_queue(spider, slot)
                break

    def _download(self, slot, request, spider):
        # The order is very important for the following deferreds. Do not change!

        # 1. Create the download deferred
        dfd = mustbe_deferred(self.handlers.download_request, request, spider) #用这个部分下载

        # 2. Notify response_downloaded listeners about the recent download
        # before querying queue for next request
        def _downloaded(response): #发信号
            self.signals.send_catch_log(signal=signals.response_downloaded,
                                        response=response,
                                        request=request,
                                        spider=spider)
            return response
        dfd.addCallback(_downloaded)

        # 3. After response arrives, remove the request from transferring
        # state to free up the transferring slot so it can be used by the
        # following requests (perhaps those which came from the downloader
        # middleware itself)
        slot.transferring.add(request) # slot管理

        def finish_transferring(_):
            slot.transferring.remove(request)
            self._process_queue(spider, slot) #调起下一次取出
            self.signals.send_catch_log(signal=signals.request_left_downloader,
                                        request=request,
                                        spider=spider)
            return _

        return dfd.addBoth(finish_transferring)

    def close(self):
        self._slot_gc_loop.stop()
        for slot in self.slots.values():
            slot.close()

    def _slot_gc(self, age=60):
        mintime = time() - age
        for key, slot in list(self.slots.items()):
            if not slot.active and slot.lastseen + slot.delay < mintime:
                self.slots.pop(key).close()
