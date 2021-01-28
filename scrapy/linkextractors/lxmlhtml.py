"""
Link extractor based on lxml.html
"""
import operator
from functools import partial
from urllib.parse import urljoin

import lxml.etree as etree
from w3lib.html import strip_html5_whitespace
from w3lib.url import canonicalize_url, safe_url_string

from scrapy.link import Link
from scrapy.linkextractors import FilteringLinkExtractor
from scrapy.utils.misc import arg_to_iter, rel_has_nofollow
from scrapy.utils.python import unique as unique_list
from scrapy.utils.response import get_base_url


# from lxml/src/lxml/html/__init__.py
XHTML_NAMESPACE = "http://www.w3.org/1999/xhtml"

_collect_string_content = etree.XPath("string()")


def _nons(tag): #将 tag 转化为复合格式的str
    if isinstance(tag, str):
        if tag[0] == '{' and tag[1:len(XHTML_NAMESPACE) + 1] == XHTML_NAMESPACE:
            return tag.split('}')[-1]
    return tag


def _identity(x):
    return x


def _canonicalize_link_url(link):
    return canonicalize_url(link.url, keep_fragments=True)


class LxmlParserLinkExtractor:
    def __init__(
        self, tag="a", attr="href", process=None, unique=False, strip=True, canonicalized=False
    ):
        self.scan_tag = tag if callable(tag) else partial(operator.eq, tag) #如果传过来的不是 in 那么就用等于tag
        self.scan_attr = attr if callable(attr) else partial(operator.eq, attr)
        self.process_attr = process if callable(process) else _identity #lambda x:x 来包装这个是个函数
        self.unique = unique
        self.strip = strip
        self.link_key = operator.attrgetter("url") if canonicalized else _canonicalize_link_url # 这个operator.attrgetter("url") .url 相当于lambda x:x.url

    def _iter_links(self, document):
        for el in document.iter(etree.Element):#遍历该节点下所有的元素
            if not self.scan_tag(_nons(el.tag)): # 如果不在目标 tags 里就跳过
                continue
            attribs = el.attrib #拿到所有 attribs
            for attrib in attribs:
                if not self.scan_attr(attrib): #如果不在目标attrib里 跳过
                    continue
                yield (el, attrib, attribs[attrib])#否则抛出这个对象

    def _extract_links(self, selector, response_url, response_encoding, base_url):
        links = []
        # hacky way to get the underlying lxml parsed document
        for el, attr, attr_val in self._iter_links(selector.root):
            # pseudo lxml.html.HtmlElement.make_links_absolute(base_url)
            try:
                if self.strip:
                    attr_val = strip_html5_whitespace(attr_val)
                attr_val = urljoin(base_url, attr_val)
            except ValueError:
                continue  # skipping bogus links
            else: #在不发生 except try正常调用后使用 finally 是不论如何都用 关闭资源
                url = self.process_attr(attr_val)
                if url is None:
                    continue
            url = safe_url_string(url, encoding=response_encoding)
            # to fix relative links after process_value
            url = urljoin(response_url, url) # 用这个可以自动检测是否有base_url 避免string 的两次叠加带来的错误
            link = Link(url, _collect_string_content(el) or '',
                        nofollow=rel_has_nofollow(el.get('rel')))
            links.append(link)
        return self._deduplicate_if_needed(links)

    def extract_links(self, response):
        base_url = get_base_url(response)
        return self._extract_links(response.selector, response.url, response.encoding, base_url)

    def _process_links(self, links):
        """ Normalize and filter extracted links

        The subclass should override it if neccessary
        """
        return self._deduplicate_if_needed(links)

    def _deduplicate_if_needed(self, links):
        if self.unique:
            return unique_list(links, key=self.link_key)
        return links


class LxmlLinkExtractor(FilteringLinkExtractor):

    def __init__(
        self,
        allow=(),
        deny=(),
        allow_domains=(),
        deny_domains=(),
        restrict_xpaths=(),
        tags=('a', 'area'),
        attrs=('href',),
        canonicalize=False,
        unique=True,
        process_value=None,
        deny_extensions=None,
        restrict_css=(),
        strip=True,
        restrict_text=None,
    ):
        tags, attrs = set(arg_to_iter(tags)), set(arg_to_iter(attrs))
        lx = LxmlParserLinkExtractor(
            tag=partial(operator.contains, tags), #他这个意思是 在 contains里 先填入一个 tags 然后返回这个处理过的函数  contains（a,b）是判断b是否在a里
            attr=partial(operator.contains, attrs), #完全同上
            unique=unique,
            process=process_value,
            strip=strip,
            canonicalized=canonicalize
        )
        super().__init__(
            link_extractor=lx,
            allow=allow,
            deny=deny,
            allow_domains=allow_domains,
            deny_domains=deny_domains,
            restrict_xpaths=restrict_xpaths,
            restrict_css=restrict_css,
            canonicalize=canonicalize,
            deny_extensions=deny_extensions,
            restrict_text=restrict_text,
        )

    def extract_links(self, response):
        """Returns a list of :class:`~scrapy.link.Link` objects from the
        specified :class:`response <scrapy.http.Response>`.

        Only links that match the settings passed to the ``__init__`` method of
        the link extractor are returned.

        Duplicate links are omitted.
        """
        base_url = get_base_url(response)
        if self.restrict_xpaths:
            docs = [
                subdoc
                for x in self.restrict_xpaths #大循环 有多少条restrict_xpaths
                for subdoc in response.xpath(x) #小循环 符合这个xpath的条目的 有多少个字条目
            ]#[a for i in range(3) for a in range(10)]》》》[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
        else:
            docs = [response.selector]
        all_links = []
        for doc in docs:
            links = self._extract_links(doc, response.url, response.encoding, base_url) #实际上是调用 LxmlParserLinkExtractor._extract_links
            all_links.extend(self._process_links(links))
        return unique_list(all_links)
