from collections import defaultdict, deque
import logging
import pprint

from scrapy.exceptions import NotConfigured
from scrapy.utils.misc import create_instance, load_object
from scrapy.utils.defer import process_parallel, process_chain, process_chain_both

logger = logging.getLogger(__name__)


class MiddlewareManager:
    """Base class for implementing middleware managers"""

    component_name = 'foo middleware'

    def __init__(self, *middlewares):
        self.middlewares = middlewares
        self.methods = defaultdict(deque) #双向que 意思是可以在左右两侧压入
        for mw in middlewares:
            self._add_middleware(mw)

    @classmethod
    def _get_mwlist_from_settings(cls, settings):
        raise NotImplementedError

    #初始化manager内部所有的中间件 然后将其添加到管理列表里 最后实例化manager自己 相当于 __init__
    @classmethod
    def from_settings(cls, settings, crawler=None):
        mwlist = cls._get_mwlist_from_settings(settings) #调用子类的_get_mwlist_from_settings 获取对应的middlewarelist
        middlewares = []
        enabled = []
        for clspath in mwlist:
            try:
                mwcls = load_object(clspath) #这里是拿到类
                mw = create_instance(mwcls, settings, crawler) #这里是创建实例
                middlewares.append(mw) #注册实例
                enabled.append(clspath) #注册类
            except NotConfigured as e:
                if e.args:
                    clsname = clspath.split('.')[-1]
                    logger.warning("Disabled %(clsname)s: %(eargs)s",
                                   {'clsname': clsname, 'eargs': e.args[0]},
                                   extra={'crawler': crawler})

        logger.info("Enabled %(componentname)ss:\n%(enabledlist)s",
                    {'componentname': cls.component_name,
                     'enabledlist': pprint.pformat(enabled)},
                    extra={'crawler': crawler})
        return cls(*middlewares)

    @classmethod
    def from_crawler(cls, crawler):
        return cls.from_settings(crawler.settings, crawler)
    #将 middleware中的openspider 和closespider 注册到self.methods里面
    def _add_middleware(self, mw):
        if hasattr(mw, 'open_spider'):
            self.methods['open_spider'].append(mw.open_spider)
        if hasattr(mw, 'close_spider'):
            self.methods['close_spider'].appendleft(mw.close_spider)

    def _process_parallel(self, methodname, obj, *args):
        return process_parallel(self.methods[methodname], obj, *args)

    def _process_chain(self, methodname, obj, *args):
        return process_chain(self.methods[methodname], obj, *args) # 这里的process_chain 实际上就是将self.methods里所有的callback添加到 一个deferred对象上 然后返回这个对象 参数是这个obj 和*args

    def _process_chain_both(self, cb_methodname, eb_methodname, obj, *args):
        return process_chain_both(self.methods[cb_methodname],
                                  self.methods[eb_methodname], obj, *args)

    def open_spider(self, spider):
        return self._process_parallel('open_spider', spider) #并发调用所有 中间中 的open_spider 方法

    def close_spider(self, spider):
        return self._process_parallel('close_spider', spider)
