from zope.interface import Interface

#接口只定义规范，没有具体实现，抽象类可以包含实现
class ISpiderLoader(Interface):

    def from_settings(settings):
        """Return an instance of the class for the given settings"""

    def load(spider_name):
        """Return the Spider class for the given spider name. If the spider
        name is not found, it must raise a KeyError."""

    def list():
        """Return a list with the names of all spiders available in the
        project"""

    def find_by_request(request):
        """Return the list of spiders names that can handle the given request"""
