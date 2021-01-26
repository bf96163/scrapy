"""Scrapy Shell

See documentation in docs/topics/shell.rst

"""
import os
import signal

from itemadapter import is_item
from twisted.internet import threads, defer
from twisted.python import threadable
from w3lib.url import any_to_uri

from scrapy.crawler import Crawler
from scrapy.exceptions import IgnoreRequest
from scrapy.http import Request, Response
from scrapy.settings import Settings
from scrapy.spiders import Spider
from scrapy.utils.conf import get_config
from scrapy.utils.console import DEFAULT_PYTHON_SHELLS, start_python_console
from scrapy.utils.datatypes import SequenceExclude
from scrapy.utils.misc import load_object
from scrapy.utils.response import open_in_browser

# 使用scrapy shell 时候用的这个类
class Shell:

    relevant_classes = (Crawler, Spider, Request, Response, Settings)

    def __init__(self, crawler, update_vars=None, code=None):
        self.crawler = crawler
        self.update_vars = update_vars or (lambda x: None)
        self.item_class = load_object(crawler.settings['DEFAULT_ITEM_CLASS'])
        self.spider = None
        self.inthread = not threadable.isInIOThread() #Are we in the thread responsible for I/O requests (the event loop)?
        self.code = code
        self.vars = {}

    def start(self, url=None, request=None, response=None, spider=None, redirect=True):
        # disable accidental Ctrl-C key press from shutting down the engine
        signal.signal(signal.SIGINT, signal.SIG_IGN)
        if url:
            self.fetch(url, spider, redirect=redirect) #有URL就爬URL
        elif request:
            self.fetch(request, spider) # 有request 就爬request
        elif response:
            request = response.request
            self.populate_vars(response, request, spider)
        else:
            self.populate_vars()
        if self.code:
            print(eval(self.code, globals(), self.vars)) # eval就是将字符串 第一个参数是 字符串 第二个是globals 第三个是locals
        else: #这里 如果没有传入code的话 这里从 环境变量和scrapy.cfg文件中读取和拿到想要的Pythonshell
            """
            Detect interactive shell setting in scrapy.cfg
            e.g.: ~/.config/scrapy.cfg or ~/.scrapy.cfg
            [settings]
            # shell can be one of ipython, bpython or python;
            # to be used as the interactive python console, if available.
            # (default is ipython, fallbacks in the order listed above)
            shell = python
            """
            cfg = get_config() #scrapy.cfg的配置
            section, option = 'settings', 'shell'
            env = os.environ.get('SCRAPY_PYTHON_SHELL')
            shells = []
            if env:
                shells += env.strip().lower().split(',')
            elif cfg.has_option(section, option):
                shells += [cfg.get(section, option).strip().lower()]
            else:  # try all by default
                shells += DEFAULT_PYTHON_SHELLS.keys()
            # always add standard shell as fallback
            shells += ['python']
            start_python_console(self.vars, shells=shells,
                                 banner=self.vars.pop('banner', '')) #开启shell

    def _schedule(self, request, spider):
        spider = self._open_spider(request, spider) #拿到spider 类实体
        d = _request_deferred(request) #将request 包装为deferred对象
        d.addCallback(lambda x: (x, spider))
        self.crawler.engine.crawl(request, spider) #调用engine 爬结果 返回Deferred
        return d

    def _open_spider(self, request, spider):
        if self.spider:
            return self.spider

        if spider is None:
            spider = self.crawler.spider or self.crawler._create_spider()

        self.crawler.spider = spider
        self.crawler.engine.open_spider(spider, close_if_idle=False)
        self.spider = spider
        return spider

    def fetch(self, request_or_url, spider=None, redirect=True, **kwargs): #用recator的 blockingCallFromThread 阻塞式执行request 操作
        from twisted.internet import reactor
        if isinstance(request_or_url, Request):
            request = request_or_url
        else:
            url = any_to_uri(request_or_url)
            request = Request(url, dont_filter=True, **kwargs)
            if redirect:
                request.meta['handle_httpstatus_list'] = SequenceExclude(range(300, 400))
            else:
                request.meta['handle_httpstatus_all'] = True
        response = None
        try:
            response, spider = threads.blockingCallFromThread(
                reactor, self._schedule, request, spider) #实际逻辑是调用 self._schedule
        except IgnoreRequest:
            pass
        self.populate_vars(response, request, spider) #更新变量表

    def populate_vars(self, response=None, request=None, spider=None):
        import scrapy
        #将 spider request 和respond 存到 self.vars这边靓丽
        self.vars['scrapy'] = scrapy
        self.vars['crawler'] = self.crawler
        self.vars['item'] = self.item_class()
        self.vars['settings'] = self.crawler.settings
        self.vars['spider'] = spider
        self.vars['request'] = request
        self.vars['response'] = response
        if self.inthread:
            self.vars['fetch'] = self.fetch #这个是个方法
        self.vars['view'] = open_in_browser
        self.vars['shelp'] = self.print_help
        self.update_vars(self.vars)
        if not self.code:
            self.vars['banner'] = self.get_help()

    def print_help(self):
        print(self.get_help())

    def get_help(self): #返回所有可用的命令和 动作的help字串
        b = []
        b.append("Available Scrapy objects:")
        b.append("  scrapy     scrapy module (contains scrapy.Request, scrapy.Selector, etc)")
        for k, v in sorted(self.vars.items()):
            if self._is_relevant(v): #筛选是否是目标类里的
                b.append(f"  {k:<10} {v}")
        b.append("Useful shortcuts:")
        if self.inthread:
            b.append("  fetch(url[, redirect=True]) "
                     "Fetch URL and update local objects (by default, redirects are followed)")
            b.append("  fetch(req)                  "
                     "Fetch a scrapy.Request and update local objects ")
        b.append("  shelp()           Shell help (print this help)")
        b.append("  view(response)    View response in a browser")

        return "\n".join(f"[s] {line}" for line in b)

    def _is_relevant(self, value):
        return isinstance(value, self.relevant_classes) or is_item(value)


def inspect_response(response, spider):
    """Open a shell to inspect the given response"""
    Shell(spider.crawler).start(response=response, spider=spider)


def _request_deferred(request):
    """Wrap a request inside a Deferred.

    This function is harmful, do not use it until you know what you are doing.

    This returns a Deferred whose first pair of callbacks are the request
    callback and errback. The Deferred also triggers when the request
    callback/errback is executed (i.e. when the request is downloaded)

    WARNING: Do not call request.replace() until after the deferred is called.
    """
    request_callback = request.callback
    request_errback = request.errback

    def _restore_callbacks(result):
        request.callback = request_callback
        request.errback = request_errback
        return result

    d = defer.Deferred()
    d.addBoth(_restore_callbacks)
    if request.callback:
        d.addCallbacks(request.callback, request.errback)

    request.callback, request.errback = d.callback, d.errback
    return d
