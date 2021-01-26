"""
Scheduler queues
"""

import marshal
import os
import pickle

from queuelib import queue

from scrapy.utils.reqser import request_to_dict, request_from_dict


def _with_mkdir(queue_class):
    # 就是返回一个传递过来的类的子类，唯一操作的事是将 __init__方法加入了一个创建目录的命令
    class DirectoriesCreated(queue_class):

        def __init__(self, path, *args, **kwargs):
            dirname = os.path.dirname(path)
            if not os.path.exists(dirname):
                os.makedirs(dirname, exist_ok=True)

            super().__init__(path, *args, **kwargs)

    return DirectoriesCreated


def _serializable_queue(queue_class, serialize, deserialize):
    # 传入一个 queue_class 继承push 和 pop方法 唯一不同的是 用serialize 和deserialize 处理pop 和push的数据
    class SerializableQueue(queue_class):

        def push(self, obj):
            s = serialize(obj)
            super().push(s)

        def pop(self):
            s = super().pop()
            if s:
                return deserialize(s)

    return SerializableQueue


def _scrapy_serialization_queue(queue_class):

    class ScrapyRequestQueue(queue_class):
    # 将传入的crawler的sipder类弄出来
        def __init__(self, crawler, key):
            self.spider = crawler.spider
            super().__init__(key)
        # 添加from_crawler
        @classmethod
        def from_crawler(cls, crawler, key, *args, **kwargs):
            return cls(crawler, key)

        def push(self, request):
            request = request_to_dict(request, self.spider) #将request 编程dict
            return super().push(request)

        def pop(self):
            request = super().pop()

            if not request:
                return None

            request = request_from_dict(request, self.spider)# 将dict 变回 request
            return request

    return ScrapyRequestQueue


def _scrapy_non_serialization_queue(queue_class):
    # 仅添加form_crawler
    class ScrapyRequestQueue(queue_class):
        @classmethod
        def from_crawler(cls, crawler, *args, **kwargs):
            return cls()

    return ScrapyRequestQueue


def _pickle_serialize(obj):
    try:
        return pickle.dumps(obj, protocol=4)
    # Both pickle.PicklingError and AttributeError can be raised by pickle.dump(s)
    # TypeError is raised from parsel.Selector
    except (pickle.PicklingError, AttributeError, TypeError) as e:
        raise ValueError(str(e)) from e


PickleFifoDiskQueueNonRequest = _serializable_queue(
    _with_mkdir(queue.FifoDiskQueue),
    _pickle_serialize,
    pickle.loads
)
PickleLifoDiskQueueNonRequest = _serializable_queue(
    _with_mkdir(queue.LifoDiskQueue),
    _pickle_serialize,
    pickle.loads
)
MarshalFifoDiskQueueNonRequest = _serializable_queue(
    _with_mkdir(queue.FifoDiskQueue),
    marshal.dumps,
    marshal.loads
)
MarshalLifoDiskQueueNonRequest = _serializable_queue(
    _with_mkdir(queue.LifoDiskQueue),
    marshal.dumps,
    marshal.loads
)

PickleFifoDiskQueue = _scrapy_serialization_queue(
    PickleFifoDiskQueueNonRequest
)
PickleLifoDiskQueue = _scrapy_serialization_queue(
    PickleLifoDiskQueueNonRequest
)
MarshalFifoDiskQueue = _scrapy_serialization_queue(
    MarshalFifoDiskQueueNonRequest
)
MarshalLifoDiskQueue = _scrapy_serialization_queue(
    MarshalLifoDiskQueueNonRequest
)
FifoMemoryQueue = _scrapy_non_serialization_queue(queue.FifoMemoryQueue)
LifoMemoryQueue = _scrapy_non_serialization_queue(queue.LifoMemoryQueue)
