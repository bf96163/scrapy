from scrapy.commands import BaseRunSpiderCommand
from scrapy.exceptions import UsageError


class Command(BaseRunSpiderCommand):

    requires_project = True

    def syntax(self):
        return "[options] <spider>"

    def short_desc(self):
        return "Run a spider"

    def run(self, args, opts):
        if len(args) < 1:
            raise UsageError()
        elif len(args) > 1:
            raise UsageError("running 'scrapy crawl' with more than one spider is no longer supported")
        spname = args[0]

        crawl_defer = self.crawler_process.crawl(spname, **opts.spargs) #调用 crawler_process.crawl 生成deferred

        if getattr(crawl_defer, 'result', None) is not None and issubclass(crawl_defer.result.type, Exception):
            self.exitcode = 1
        else:
            self.crawler_process.start() #调用 crawl_process >> CrawlerRunner >>  crawler 的start方法 （调起recator）

            if (
                self.crawler_process.bootstrap_failed
                or hasattr(self.crawler_process, 'has_exception') and self.crawler_process.has_exception
            ):
                self.exitcode = 1
