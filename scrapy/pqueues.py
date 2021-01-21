import hashlib
import logging

from scrapy.utils.misc import create_instance

logger = logging.getLogger(__name__)


def _path_safe(text):
    """
    Return a filesystem-safe version of a string ``text``
    返回一个精简符号后的 字符串后面跟一个十六进制的摘要信息（摘要是原文）
    e.g.  simple.org@com => simple.org_com-12312841a6d777e87c3e
    >>> _path_safe('simple.org').startswith('simple.org')
    True
    >>> _path_safe('dash-underscore_.org').startswith('dash-underscore_.org')
    True
    >>> _path_safe('some@symbol?').startswith('some_symbol_')
    True
    """
    # .isalnum()判断的是是否是字母和数字，大致的意思就是将除了-._以外的特殊符号都换成_
    pathable_slot = "".join([c if c.isalnum() or c in '-._' else '_'
                             for c in text])
    # as we replace some letters we can get collision for different slots
    # add we add unique part .hexdigest()返回摘要，作为十六进制数据字符串值
    unique_slot = hashlib.md5(text.encode('utf8')).hexdigest()
    return '-'.join([pathable_slot, unique_slot])


class ScrapyPriorityQueue:
    """A priority queue implemented using multiple internal queues (typically,
    FIFO queues). It uses one internal queue for each priority value. The internal
    queue must implement the following methods:
    优先级队列实际上是由多个队列聚合而成的队列，每个优先级数值对应一个FIFO队列，需要实现以下方法:
        * push(obj)
        * pop()
        * close()
        * __len__()

    ``__init__`` method of ScrapyPriorityQueue receives a downstream_queue_cls
    argument, which is a class used to instantiate a new (internal) queue when
    a new priority is allocated.
    在`__init__`函数里，接受一个“下游que类（子que类）”作为参数，当新的优先级出现时候，用这个类实例化新的que

    Only integer priorities should be used. Lower numbers are higher
    priorities. 只接受整数优先级值，数值越小 优先级越大 （指的是当调用时候）

    startprios is a sequence of priorities to start with. If the queue was
    previously closed leaving some priority buckets non-empty, those priorities
    should be passed in startprios.
    starprios是一系列优先级数值，当之前的关闭工作剩余了一些优先级应该加入到这个值里传递进来

    """

    @classmethod
    def from_crawler(cls, crawler, downstream_queue_cls, key, startprios=()):
        return cls(crawler, downstream_queue_cls, key, startprios) #返回的实际上就是本类

    def __init__(self, crawler, downstream_queue_cls, key, startprios=()):
        self.crawler = crawler
        self.downstream_queue_cls = downstream_queue_cls
        self.key = key
        self.queues = {}
        self.curprio = None
        self.init_prios(startprios)

    def init_prios(self, startprios):
        if not startprios:  #如果starprios 为 list notlist 为否 则跳过 为空 则直接返回
            return

        for priority in startprios:
            self.queues[priority] = self.qfactory(priority)

        self.curprio = min(startprios) #数值越小 优先级越高 （注意这里的优先级的值是由上次关闭的que传入的原始值 ）
    # 生成对应优先级的que的实例
    def qfactory(self, key):
        return create_instance(self.downstream_queue_cls, #create_instance 调用其自身的实例化方法实例化本体
                               None,
                               self.crawler,
                               self.key + '/' + str(key))

    def priority(self, request): #传入数值会正负值翻转
        return -request.priority
    # 按照优先级 压入一个request
    def push(self, request):
        priority = self.priority(request) # 从request的角度讲  发送的数值绝对值越大 翻转后的数值越小
        if priority not in self.queues:
            self.queues[priority] = self.qfactory(priority)
        q = self.queues[priority]
        q.push(request)  # this may fail (eg. serialization error)
        if self.curprio is None or priority < self.curprio: #que内部是 值越小优先级越大 发送以后的值会翻转
            self.curprio = priority
    # 弹出一个request （当其内部为0的时候挂壁这个que）
    def pop(self):
        if self.curprio is None:
            return
        q = self.queues[self.curprio]
        m = q.pop() #取出request
        if not q:
            del self.queues[self.curprio] #从管理列表中移出
            q.close() #关闭que
            prios = [p for p, q in self.queues.items() if q] #从管理列表中取出优先级值列表
            self.curprio = min(prios) if prios else None #设定新的 self.curprio
        return m
    # 关闭剩余QUE 并返回对应优先级数字（是已经正负取反的）
    def close(self):
        active = []
        for p, q in self.queues.items():
            active.append(p) #仅仅保留优先级值，不保留requests
            q.close()
        return active #返回优先级值列表

    def __len__(self):
        return sum(len(x) for x in self.queues.values()) if self.queues else 0


class DownloaderInterface: #检测传入 crawler 的
    #目前看起来有点像 select模块 就是检测 这个crawler下的downloader 下的 slots 的状态
    def __init__(self, crawler):
        self.downloader = crawler.engine.downloader

    def stats(self, possible_slots): #返回 给出的接口列表里 每个接口的request个数
        return [(self._active_downloads(slot), slot)
                for slot in possible_slots]

    def get_slot_key(self, request): #给定一个rqeust 返回 slotkey
        return self.downloader._get_slot_key(request, None)

    def _active_downloads(self, slot): #返回对应接口的requests的个数
        """ Return a number of requests in a Downloader for a given slot """
        if slot not in self.downloader.slots:
            return 0
        return len(self.downloader.slots[slot].active)


class DownloaderAwarePriorityQueue:
    """ PriorityQueue which takes Downloader activity into account:
    domains (slots) with the least amount of active downloads are dequeued
    first. 下载器的活跃程度 活跃程度低下的下载器 先从que中拿东西。
    """

    @classmethod
    def from_crawler(cls, crawler, downstream_queue_cls, key, startprios=()):
        return cls(crawler, downstream_queue_cls, key, startprios)

    def __init__(self, crawler, downstream_queue_cls, key, slot_startprios=()):
        if crawler.settings.getint('CONCURRENT_REQUESTS_PER_IP') != 0:
            raise ValueError(f'"{self.__class__}" does not support CONCURRENT_REQUESTS_PER_IP')

        if slot_startprios and not isinstance(slot_startprios, dict):
            raise ValueError("DownloaderAwarePriorityQueue accepts "
                             "``slot_startprios`` as a dict; "
                             f"{slot_startprios.__class__!r} instance "
                             "is passed. Most likely, it means the state is"
                             "created by an incompatible priority queue. "
                             "Only a crawl started with the same priority "
                             "queue class can be resumed.")

        self._downloader_interface = DownloaderInterface(crawler)
        self.downstream_queue_cls = downstream_queue_cls
        self.key = key
        self.crawler = crawler

        self.pqueues = {}  # slot -> priority queue
        for slot, startprios in (slot_startprios or {}).items():
            self.pqueues[slot] = self.pqfactory(slot, startprios)

    def pqfactory(self, slot, startprios=()): #实际上用的que就是上面的优先级que
        return ScrapyPriorityQueue(self.crawler,
                                   self.downstream_queue_cls,
                                   self.key + '/' + _path_safe(slot),
                                   startprios)

    def pop(self):
        stats = self._downloader_interface.stats(self.pqueues) #从crawler 的downloader 里面拿到 每个slot下request数量列表

        if not stats:
            return

        slot = min(stats)[1] # 找到数量最小的slot
        queue = self.pqueues[slot]
        request = queue.pop()
        if len(queue) == 0:
            del self.pqueues[slot]
        return request

    def push(self, request):
        slot = self._downloader_interface.get_slot_key(request) #拿到对应request的slot
        if slot not in self.pqueues:
            self.pqueues[slot] = self.pqfactory(slot)
        queue = self.pqueues[slot]
        queue.push(request)

    def close(self):
        active = {slot: queue.close()
                  for slot, queue in self.pqueues.items()}
        self.pqueues.clear()
        return active

    def __len__(self):
        return sum(len(x) for x in self.pqueues.values()) if self.pqueues else 0 #返回所有que里面的 request 个数

    def __contains__(self, slot): #判断for in 的
        return slot in self.pqueues
