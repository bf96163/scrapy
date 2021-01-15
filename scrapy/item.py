"""
Scrapy Item

See documentation in docs/topics/item.rst
"""

from abc import ABCMeta
from collections.abc import MutableMapping
from copy import deepcopy
from pprint import pformat
from warnings import warn

from scrapy.utils.deprecate import ScrapyDeprecationWarning
from scrapy.utils.trackref import object_ref


class _BaseItem(object_ref):
    """
    Temporary class used internally to avoid the deprecation
    warning raised by isinstance checks using BaseItem.
    """
    pass


class _BaseItemMeta(ABCMeta): #抽象类，如果用修饰器@abc.abstractmethod  修饰某个方法，则继承这个_BaseitemMeta 的类需要实现那个方法。
    def __instancecheck__(cls, instance): #在调用 isinstance时候回调用这个方法 魔法函数 跟__string__类似
        if cls is BaseItem:
            warn('scrapy.item.BaseItem is deprecated, please use scrapy.item.Item instead',
                 ScrapyDeprecationWarning, stacklevel=2)
        return super().__instancecheck__(instance)


class BaseItem(_BaseItem, metaclass=_BaseItemMeta):
    """
    Deprecated, please use :class:`scrapy.item.Item` instead
    """

    def __new__(cls, *args, **kwargs):
        if issubclass(cls, BaseItem) and not issubclass(cls, (Item, DictItem)):
            warn('scrapy.item.BaseItem is deprecated, please use scrapy.item.Item instead',
                 ScrapyDeprecationWarning, stacklevel=2)
        return super().__new__(cls, *args, **kwargs)


class Field(dict):
    """Container of field metadata"""


class ItemMeta(_BaseItemMeta):
    """Metaclass_ of :class:`Item` that handles field definitions.

    .. _metaclass: https://realpython.com/python-metaclasses
    """
    # 整体的作用就是 将自身的类 改个名字放到_class里 将所有的Field类型属性放到 fields属性里，估计是方便调用 （需要相应的类使用这个元类，起到一个整理的作用）
    def __new__(mcs, class_name, bases, attrs): #牢记__new__是一个类方法 @classmethod super 以后才会调用init 不然会跳过init方法
        classcell = attrs.pop('__classcell__', None) #自定义metaclass时候 需要把这个属性传递给父meta的__new__方法  https://stackoverflow.com/questions/41343263/provide-classcell-example-for-python-3-6-metaclass
        # 简单的理解就是 将有_class这个属性时候，生成一个“x_名字” 的的跟自己差不多的类 后面叫"分身"
        new_bases = tuple(base._class for base in bases if hasattr(base, '_class'))
        _class = super().__new__(mcs, 'x_' + class_name, new_bases, attrs)# super是调用父类的生成类的方法，不会造成循环调用

        fields = getattr(_class, 'fields', {}) #从分身这个类里面拿到fields属性
        new_attrs = {}
        for n in dir(_class):#dir() 范围内的变量、方法和定义的类型列表；(包括继承过来的)
            v = getattr(_class, n)
            if isinstance(v, Field):
                fields[n] = v
            elif n in attrs:
                new_attrs[n] = attrs[n]
        # 主要是将field的值跟 其他的值区分开
        new_attrs['fields'] = fields
        new_attrs['_class'] = _class
        #下面这部分是python3.6以后自己写的metaclass.__new__需要传递__classcell__这个值
        if classcell is not None:
            new_attrs['__classcell__'] = classcell
        return super().__new__(mcs, class_name, bases, new_attrs)

# Mutablemapping 继承自Mapping, 添加了抽象方法__setitem__()和__delitem__()。还添加了pop()、popitem()、clear()、update()和setdefault()的实现。
class DictItem(MutableMapping, BaseItem):

    fields = {}
    # 增加个warning而已没用
    def __new__(cls, *args, **kwargs):
        if issubclass(cls, DictItem) and not issubclass(cls, Item):
            warn('scrapy.item.DictItem is deprecated, please use scrapy.item.Item instead',
                 ScrapyDeprecationWarning, stacklevel=2)
        return super().__new__(cls, *args, **kwargs)
    # 将参数列表展开到内部
    def __init__(self, *args, **kwargs):
        self._values = {}
        if args or kwargs:  # avoid creating dict for most common case
            for k, v in dict(*args, **kwargs).items(): #只要args里的参数 和 kwargs里的字典都会展开到内部的属性和值
                self[k] = v

    #Mutablemapping 类型需要复写的
    def __getitem__(self, key):
        return self._values[key]

    # Mutablemapping 类型需要复写的
    def __setitem__(self, key, value):
        if key in self.fields:
            self._values[key] = value
        else:
            raise KeyError(f"{self.__class__.__name__} does not support field: {key}")

    # Mutablemapping 类型需要复写的
    def __delitem__(self, key):
        del self._values[key]

    # Mutablemapping 类型需要复写的
    def __getattr__(self, name):
        if name in self.fields:
            raise AttributeError(f"Use item[{name!r}] to get field value")
        raise AttributeError(name)

    def __setattr__(self, name, value):
        if not name.startswith('_'):
            raise AttributeError(f"Use item[{name!r}] = {value!r} to set field value")
        super().__setattr__(name, value)

    def __len__(self):
        return len(self._values)

    def __iter__(self):
        return iter(self._values)

    __hash__ = BaseItem.__hash__

    def keys(self):
        return self._values.keys()

    def __repr__(self):
        return pformat(dict(self))

    def copy(self):
        return self.__class__(self)

    def deepcopy(self):
        """Return a :func:`~copy.deepcopy` of this item.
        """
        return deepcopy(self)


class Item(DictItem, metaclass=ItemMeta):
    """
    Base class for scraped items.

    In Scrapy, an object is considered an ``item`` if it is an instance of either
    :class:`Item` or :class:`dict`, or any subclass. For example, when the output of a
    spider callback is evaluated, only instances of :class:`Item` or
    :class:`dict` are passed to :ref:`item pipelines <topics-item-pipeline>`.

    If you need instances of a custom class to be considered items by Scrapy,
    you must inherit from either :class:`Item` or :class:`dict`.

    Items must declare :class:`Field` attributes, which are processed and stored
    in the ``fields`` attribute. This restricts the set of allowed field names
    and prevents typos, raising ``KeyError`` when referring to undefined fields.
    Additionally, fields can be used to define metadata and control the way
    data is processed internally. Please refer to the :ref:`documentation
    about fields <topics-items-fields>` for additional information.

    Unlike instances of :class:`dict`, instances of :class:`Item` may be
    :ref:`tracked <topics-leaks-trackrefs>` to debug memory leaks.
    """
