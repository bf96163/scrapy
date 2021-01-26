from twisted.internet import defer
from twisted.internet.base import ThreadedResolver
from twisted.internet.interfaces import IHostResolution, IHostnameResolver, IResolutionReceiver, IResolverSimple
from zope.interface.declarations import implementer, provider

from scrapy.utils.datatypes import LocalCache


# TODO: cache misses
dnscache = LocalCache(10000) #就是一个带有limit的 OrderedDict


@implementer(IResolverSimple) #IResolverSimple 有一个方法	getHostByName 就是将域名解析为IP. zope接口
class CachingThreadedResolver(ThreadedResolver):
    """
    Default caching resolver. IPv4 only, supports setting a timeout value for DNS requests.
    """

    def __init__(self, reactor, cache_size, timeout):
        super().__init__(reactor)
        dnscache.limit = cache_size
        self.timeout = timeout

    @classmethod
    def from_crawler(cls, crawler, reactor):
        if crawler.settings.getbool('DNSCACHE_ENABLED'): #这块可以考虑在setting里加上  这个可以减少DNS调用哈
            cache_size = crawler.settings.getint('DNSCACHE_SIZE')
        else:
            cache_size = 0
        return cls(reactor, cache_size, crawler.settings.getfloat('DNS_TIMEOUT'))

    def install_on_reactor(self):
        self.reactor.installResolver(self)

    def getHostByName(self, name, timeout=None):
        if name in dnscache:
            return defer.succeed(dnscache[name]) #直接调用 defer.succeed方法 返回对应值
        # in Twisted<=16.6, getHostByName() is always called with
        # a default timeout of 60s (actually passed as (1, 3, 11, 45) tuple),
        # so the input argument above is simply overridden
        # to enforce Scrapy's DNS_TIMEOUT setting's value
        timeout = (self.timeout,)
        d = super().getHostByName(name, timeout)# 调用父类的ThreadedResolver 的 getHostByName 拿到IP
        if dnscache.limit:
            d.addCallback(self._cache_result, name) # 添callback 到 deffer里
        return d

    def _cache_result(self, result, name):
        dnscache[name] = result #写入缓存
        return result


@implementer(IHostResolution)#IHostResolution表示正在进行的DNS名称递归查询。
class HostResolution:
    def __init__(self, name):
        self.name = name

    def cancel(self):
        raise NotImplementedError()


@provider(IResolutionReceiver)#接受域名解析的结果 由IHostnameResolver 初始化 这里相当于重写一个Receiver 带有缓存功能
class _CachingResolutionReceiver:
    def __init__(self, resolutionReceiver, hostName):
        self.resolutionReceiver = resolutionReceiver
        self.hostName = hostName
        self.addresses = []

    def resolutionBegan(self, resolution):
        self.resolutionReceiver.resolutionBegan(resolution)
        self.resolution = resolution

    def addressResolved(self, address):
        self.resolutionReceiver.addressResolved(address)
        self.addresses.append(address) #解析列表添加成功解析的东西

    def resolutionComplete(self):#相当于回调
        self.resolutionReceiver.resolutionComplete()
        if self.addresses:
            dnscache[self.hostName] = self.addresses #写入缓存


@implementer(IHostnameResolver)
class CachingHostnameResolver:
    """
    Experimental caching resolver. Resolves IPv4 and IPv6 addresses,
    does not support setting a timeout value for DNS requests.
    """

    def __init__(self, reactor, cache_size):
        self.reactor = reactor
        self.original_resolver = reactor.nameResolver
        dnscache.limit = cache_size

    @classmethod
    def from_crawler(cls, crawler, reactor):
        if crawler.settings.getbool('DNSCACHE_ENABLED'):
            cache_size = crawler.settings.getint('DNSCACHE_SIZE')
        else:
            cache_size = 0
        return cls(reactor, cache_size)

    def install_on_reactor(self):
        self.reactor.installNameResolver(self)

    def resolveHostName(
        self, resolutionReceiver, hostName, portNumber=0, addressTypes=None, transportSemantics="TCP"
    ): #尝试从缓存中解析域名 ，失败后再调用 原有方法解析
        try:
            addresses = dnscache[hostName]
        except KeyError:
            return self.original_resolver.resolveHostName(
                _CachingResolutionReceiver(resolutionReceiver, hostName),
                hostName,
                portNumber,
                addressTypes,
                transportSemantics,
            )
        else:
            resolutionReceiver.resolutionBegan(HostResolution(hostName))
            for addr in addresses:
                resolutionReceiver.addressResolved(addr)
            resolutionReceiver.resolutionComplete()
            return resolutionReceiver
