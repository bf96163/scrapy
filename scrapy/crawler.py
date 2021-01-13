import logging
import pprint
import signal
import warnings

from twisted.internet import defer
from zope.interface.exceptions import DoesNotImplement

try:
    # zope >= 5.0 only supports MultipleInvalid
    from zope.interface.exceptions import MultipleInvalid
except ImportError:
    MultipleInvalid = None

from zope.interface.verify import verifyClass

from scrapy import signals, Spider
from scrapy.core.engine import ExecutionEngine
from scrapy.exceptions import ScrapyDeprecationWarning
from scrapy.extension import ExtensionManager
from scrapy.interfaces import ISpiderLoader
from scrapy.settings import overridden_settings, Settings
from scrapy.signalmanager import SignalManager
from scrapy.utils.log import (
    configure_logging,
    get_scrapy_root_handler,
    install_scrapy_root_handler,
    log_scrapy_info,
    LogCounterHandler,
)
from scrapy.utils.misc import create_instance, load_object
from scrapy.utils.ossignal import install_shutdown_handlers, signal_names
from scrapy.utils.reactor import install_reactor, verify_installed_reactor

# scrapy 中文解释及其注释
logger = logging.getLogger(__name__)

#实际爬取执行的地方
class Crawler:

    def __init__(self, spidercls, settings=None):
        if isinstance(spidercls, Spider):
            #要求传入的是类而不是实例
            raise ValueError('The spidercls argument must be a class, not an object')

        if isinstance(settings, dict) or settings is None:
            #转化为setting对象
            settings = Settings(settings)

        self.spidercls = spidercls
        self.settings = settings.copy()
        #然后使用spidercls类的update_setting方式来更新设置
        self.spidercls.update_settings(self.settings)

        self.signals = SignalManager(self)
        #从类的setting中的STATS_CLASS拿到stats
        self.stats = load_object(self.settings['STATS_CLASS'])(self)
        #从setting中拿到loglevel 将初始化的LogCounterHandler 加入到logging.root
        handler = LogCounterHandler(self, level=self.settings.get('LOG_LEVEL'))
        logging.root.addHandler(handler)
        # 显示出来所有被复写的setting
        d = dict(overridden_settings(self.settings))
        logger.info("Overridden settings:\n%(settings)s",
                    {'settings': pprint.pformat(d)})

        if get_scrapy_root_handler() is not None:
            # scrapy root handler already installed: update it with new settings
            install_scrapy_root_handler(self.settings)
        # lambda is assigned to Crawler attribute because this way it is not
        # garbage collected after leaving __init__ scope
        self.__remove_handler = lambda: logging.root.removeHandler(handler)
        #将该signals.engine_stopped信号的callback注册到self.__remove_handler函数上
        self.signals.connect(self.__remove_handler, signals.engine_stopped)
        # log格式指定
        lf_cls = load_object(self.settings['LOG_FORMATTER'])
        self.logformatter = lf_cls.from_crawler(self)
        # 扩展 还没看
        self.extensions = ExtensionManager.from_crawler(self)

        self.settings.freeze()
        self.crawling = False
        self.spider = None
        self.engine = None
    #defer.inlineCallbacks 装饰器 是指当使用异步调用该方法时候，
    # 该方法可以用类似同步语法的方法写异步的工作，其中yield deferred对象后
    # 后续代码会等待这个yield出去的deferred成功返回后再进行下一步
    # 其中等待时间交还给reactor。
    @defer.inlineCallbacks
    def crawl(self, *args, **kwargs):
        if self.crawling:
            raise RuntimeError("Crawling already taking place")
        self.crawling = True

        try:
            self.spider = self._create_spider(*args, **kwargs)
            self.engine = self._create_engine()
            #从self.spider.start_requests()中拿到requests
            start_requests = iter(self.spider.start_requests())
            #调用异步方法，开始爬虫爬取工作
            yield self.engine.open_spider(self.spider, start_requests)
            #调用核心的start方法，并将返回值包装成deferred对象
            yield defer.maybeDeferred(self.engine.start)
        except Exception:
            self.crawling = False
            if self.engine is not None:
                yield self.engine.close()
            raise

    def _create_spider(self, *args, **kwargs):
        #调用传入spider类的from_crawler(self, args, *kwargs)
        return self.spidercls.from_crawler(self, *args, **kwargs)

    def _create_engine(self):
        #ExecutionEngine(self, lambda _: self.stop()) 传入stop函数 实例化engine
        return ExecutionEngine(self, lambda _: self.stop())

    #重置self.crawling 为false 同时发出 异步命令self.engine.stop 包装一下self.engine.stop成为deferred对象
    @defer.inlineCallbacks
    def stop(self):
        """Starts a graceful stop of the crawler and returns a deferred that is
        fired when the crawler is stopped."""
        if self.crawling:
            self.crawling = False
            yield defer.maybeDeferred(self.engine.stop)

# 简单的说 如果你自己的应用用到reactor 可以考虑用这个类控制spider启停等，不然用CrawlerProcess
class CrawlerRunner:
    """
    This is a convenient helper class that keeps track of, manages and runs
    crawlers inside an already setup :mod:`~twisted.internet.reactor`.

    The CrawlerRunner object must be instantiated with a
    :class:`~scrapy.settings.Settings` object.

    This class shouldn't be needed (since Scrapy is responsible of using it
    accordingly) unless writing scripts that manually handle the crawling
    process. See :ref:`run-from-script` for an example.
    """
    #property()函数是用来指定当前属性的文件描述符类的方法，这里就是把lambda作为 他的getter 返回的是self._crawlers
    crawlers = property(
        lambda self: self._crawlers,
        doc="Set of :class:`crawlers <scrapy.crawler.Crawler>` started by "
            ":meth:`crawl` and managed by this class."
    )
    #从setting里 建立spiderloader实例
    @staticmethod
    def _get_spider_loader(settings):
        """ Get SpiderLoader instance from settings """
        cls_path = settings.get('SPIDER_LOADER_CLASS')
        #load_object(cls_path) 是将xx.xx路径转为实例的类
        loader_cls = load_object(cls_path)
        excs = (DoesNotImplement, MultipleInvalid) if MultipleInvalid else DoesNotImplement
        try:
            verifyClass(ISpiderLoader, loader_cls)
        except excs:
            warnings.warn(
                'SPIDER_LOADER_CLASS (previously named SPIDER_MANAGER_CLASS) does '
                'not fully implement scrapy.interfaces.ISpiderLoader interface. '
                'Please add all missing methods to avoid unexpected runtime errors.',
                category=ScrapyDeprecationWarning, stacklevel=2
            )
        #这里返回的是 setting中spiderloader加载目前的setting后的实例
        return loader_cls.from_settings(settings.frozencopy())

    def __init__(self, settings=None):
        if isinstance(settings, dict) or settings is None:
            settings = Settings(settings)
        self.settings = settings
        self.spider_loader = self._get_spider_loader(settings)
        self._crawlers = set()
        self._active = set()
        self.bootstrap_failed = False
        self._handle_twisted_reactor()

    @property
    def spiders(self):
        warnings.warn("CrawlerRunner.spiders attribute is renamed to "
                      "CrawlerRunner.spider_loader.",
                      category=ScrapyDeprecationWarning, stacklevel=2)
        return self.spider_loader

    #生成一个crawler对象 然后调用其crawl方法，其中有些管理crawler的deferred的部分 在_crawl里
    def crawl(self, crawler_or_spidercls, *args, **kwargs):
        """
        Run a crawler with the provided arguments.

        It will call the given Crawler's :meth:`~Crawler.crawl` method, while
        keeping track of it so it can be stopped later.

        If ``crawler_or_spidercls`` isn't a :class:`~scrapy.crawler.Crawler`
        instance, this method will try to create one using this parameter as
        the spider class given to it.

        Returns a deferred that is fired when the crawling is finished.

        :param crawler_or_spidercls: already created crawler, or a spider class
            or spider's name inside the project to create it
        :type crawler_or_spidercls: :class:`~scrapy.crawler.Crawler` instance,
            :class:`~scrapy.spiders.Spider` subclass or string

        :param args: arguments to initialize the spider

        :param kwargs: keyword arguments to initialize the spider
        """
        if isinstance(crawler_or_spidercls, Spider):
            raise ValueError(
                'The crawler_or_spidercls argument cannot be a spider object, '
                'it must be a spider class (or a Crawler object)')
        crawler = self.create_crawler(crawler_or_spidercls)
        return self._crawl(crawler, *args, **kwargs)

    def _crawl(self, crawler, *args, **kwargs):
        self.crawlers.add(crawler) #这个集合里是crawler
        d = crawler.crawl(*args, **kwargs)
        self._active.add(d) # 这个集合里是执行crawl后的deferred对象

        def _done(result):
            # discard 相当于不报错的remove
            self.crawlers.discard(crawler)
            self._active.discard(d)
            #a|=2等价于a=a|2(按位或)
            self.bootstrap_failed |= not getattr(crawler, 'spider', None)
            return result
        # 运行crawl前 加入到管理集合，给其deferred对象添加结束后清理管理集合的代码
        return d.addBoth(_done)

    def create_crawler(self, crawler_or_spidercls):
        """
        Return a :class:`~scrapy.crawler.Crawler` object.

        * If ``crawler_or_spidercls`` is a Crawler, it is returned as-is.
        * If ``crawler_or_spidercls`` is a Spider subclass, a new Crawler
          is constructed for it.
        * If ``crawler_or_spidercls`` is a string, this function finds
          a spider with this name in a Scrapy project (using spider loader),
          then creates a Crawler instance for it.
        """
        if isinstance(crawler_or_spidercls, Spider):
            raise ValueError(
                'The crawler_or_spidercls argument cannot be a spider object, '
                'it must be a spider class (or a Crawler object)')
        if isinstance(crawler_or_spidercls, Crawler):
            return crawler_or_spidercls

        return self._create_crawler(crawler_or_spidercls)

    def _create_crawler(self, spidercls):
        if isinstance(spidercls, str):
            spidercls = self.spider_loader.load(spidercls)
        # 实际实例化Crawler进行的地方 传入的是spider的类和setting
        return Crawler(spidercls, self.settings)

    def stop(self):
        """
        Stops simultaneously all the crawling jobs taking place.

        Returns a deferred that is fired when they all have ended.
        """
        #跟crawler不一样，这个是一个由每个crawler执行stop函数后返回的deferred对象列表
        return defer.DeferredList([c.stop() for c in list(self.crawlers)])

    # 跟多进程的join类似，等待所有crawler完成任务
    # 上面说到这个_active集合是所有crawler的deferred对象 把他们yield出去 以便调用后续callback
    @defer.inlineCallbacks
    def join(self):
        """
        join()

        Returns a deferred that is fired when all managed :attr:`crawlers` have
        completed their executions.
        """
        while self._active:
            yield defer.DeferredList(self._active)
    # 这个方法返回从传入setting 字符中加载 用 load_object()加载进来的recator实例 或者啥也不做
    def _handle_twisted_reactor(self):
        if self.settings.get("TWISTED_REACTOR"):
            verify_installed_reactor(self.settings["TWISTED_REACTOR"])

#这个crawlerProcess 是用来在不用recator的应用里使用 同时在一个进程里使用多个spider的
# 如果只是使用scrapy就不用改这个，除非想把scrapy放到你自己应用里　
# 实际上这个是上一个 CrawlerRunner 添加了reactor后的东西
class CrawlerProcess(CrawlerRunner):
    """
    A class to run multiple scrapy crawlers in a process simultaneously.

    This class extends :class:`~scrapy.crawler.CrawlerRunner` by adding support
    for starting a :mod:`~twisted.internet.reactor` and handling shutdown
    signals, like the keyboard interrupt command Ctrl-C. It also configures
    top-level logging.

    This utility should be a better fit than
    :class:`~scrapy.crawler.CrawlerRunner` if you aren't running another
    :mod:`~twisted.internet.reactor` within your application.

    The CrawlerProcess object must be instantiated with a
    :class:`~scrapy.settings.Settings` object.

    :param install_root_handler: whether to install root logging handler
        (default: True)

    This class shouldn't be needed (since Scrapy is responsible of using it
    accordingly) unless writing scripts that manually handle the crawling
    process. See :ref:`run-from-script` for an example.
    """

    def __init__(self, settings=None, install_root_handler=True):
        super().__init__(settings)
        #将shutdownhandler 加载为函数_signal_shutdown
        install_shutdown_handlers(self._signal_shutdown)
        configure_logging(self.settings, install_root_handler)
        log_scrapy_info(self.settings)

    def _signal_shutdown(self, signum, _):
        from twisted.internet import reactor
        #将shutdownhandler注册为 _signal_kill
        install_shutdown_handlers(self._signal_kill)
        signame = signal_names[signum]
        logger.info("Received %(signame)s, shutting down gracefully. Send again to force ",
                    {'signame': signame})
        #使用reactor.callFromThread(self._graceful_stop_reactor)命令调用自身的结束语句
        reactor.callFromThread(self._graceful_stop_reactor)

    def _signal_kill(self, signum, _):
        from twisted.internet import reactor
        #将shutdownhandler注册为signal.SIG_IGN
        install_shutdown_handlers(signal.SIG_IGN)
        signame = signal_names[signum]
        logger.info('Received %(signame)s twice, forcing unclean shutdown',
                    {'signame': signame})
        #直接将recator关闭
        reactor.callFromThread(self._stop_reactor)

    def start(self, stop_after_crawl=True):
        """
        This method starts a :mod:`~twisted.internet.reactor`, adjusts its pool
        size to :setting:`REACTOR_THREADPOOL_MAXSIZE`, and installs a DNS cache
        based on :setting:`DNSCACHE_ENABLED` and :setting:`DNSCACHE_SIZE`.

        If ``stop_after_crawl`` is True, the reactor will be stopped after all
        crawlers have finished, using :meth:`join`.

        :param bool stop_after_crawl: stop or not the reactor when all
            crawlers have finished
        """
        from twisted.internet import reactor
        #设置如果爬完后关闭recator的话 就添加相应的callback结束callback 这就是上面join的用处 ，如果这里不设置为true
        # 那么这个reactor就会留着不销毁
        if stop_after_crawl:
            d = self.join()
            # Don't start the reactor if the deferreds are already fired
            if d.called:
                return
            d.addBoth(self._stop_reactor)
        #加载一个配置 threadpool和dns_resolver的配置到recator
        resolver_class = load_object(self.settings["DNS_RESOLVER"])
        resolver = create_instance(resolver_class, self.settings, self, reactor=reactor)
        resolver.install_on_reactor()
        tp = reactor.getThreadPool()
        tp.adjustPoolsize(maxthreads=self.settings.getint('REACTOR_THREADPOOL_MAXSIZE'))
        reactor.addSystemEventTrigger('before', 'shutdown', self.stop)
        #启动reactor并阻塞
        reactor.run(installSignalHandlers=False)  # blocking call

    def _graceful_stop_reactor(self):
        # 给所有crawler 的deferred对象后面添加一个完成后销毁的动作
        d = self.stop()
        d.addBoth(self._stop_reactor)
        return d

    def _stop_reactor(self, _=None):
        from twisted.internet import reactor
        try:
            reactor.stop()
        except RuntimeError:  # raised if already stopped or in shutdown stage
            pass

    def _handle_twisted_reactor(self):
        if self.settings.get("TWISTED_REACTOR"):
            install_reactor(self.settings["TWISTED_REACTOR"], self.settings["ASYNCIO_EVENT_LOOP"])
        super()._handle_twisted_reactor()
