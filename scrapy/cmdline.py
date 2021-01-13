import sys
import os
import optparse
import cProfile
import inspect
import pkg_resources

import scrapy
from scrapy.crawler import CrawlerProcess
from scrapy.commands import ScrapyCommand
from scrapy.exceptions import UsageError
from scrapy.utils.misc import walk_modules
from scrapy.utils.project import inside_project, get_project_settings
from scrapy.utils.python import garbage_collect


def _iter_command_classes(module_name):
    # TODO: add `name` attribute to commands and and merge this function with
    # scrapy.utils.spider.iter_spider_classes
    # XX实现从模块名字到模块的映射XX
    """ 判断传入的模块名称 ，在它的属性中判断
    是个类而且如果这个子模块是 Scrapycommand 的子类话 就抛出这个子模块
    其中walk_modules 是将一个模块内的所有可用部分列出，包括子模块的
    vars（k）函数返回的是一个ke包含的所有属性及其方法的字典 key是名字 value是值"""
    for module in walk_modules(module_name):
        for obj in vars(module).values():
            if (
                inspect.isclass(obj) #是类
                and issubclass(obj, ScrapyCommand) #是目标子类
                and obj.__module__ == module.__name__ # 是module下面的类 而不是其他模块下的类
                and not obj == ScrapyCommand #是继承后的而不是本体
            ):
                yield obj

# 给定一个模块 和是否在项目内的flag，检查_iter_command_classes（模块）返回的方法
# 也就是检查这个函数是否需要在 项目环境中运行
def _get_commands_from_module(module, inproject):
    d = {}
    for cmd in _iter_command_classes(module):
        # 在项目内时候，且目标函数不需要在项目内的时候 添加到函数字典中
        if inproject or not cmd.requires_project:#ScrapyCommand.requires_project 默认为false
            cmdname = cmd.__module__.split('.')[-1]
            d[cmdname] = cmd()
    return d

# 跟walkthrough很像，不过这里是给定一个group 返回他所有的 在group 下的 entry_point（类似于子类）
def _get_commands_from_entry_points(inproject, group='scrapy.commands'):
    cmds = {}
    # entry_point 相当于载入不同的类，比如一个类 可以由不同的参数初始化，这里就可以用
    # [qipaionweb.games]
    # doudizhu = doudizhu.game_impl:GameImpl
    # 方式来定义几个 entry_point
    # 后面函数需要调用某个不同的配置的类的时候就只需要
    # return pkg_resources.load_entry_point(doudizhu, qipaionweb.games, doudizhu) 就可以载入这个类 而不用在代码中
    # 自己做不同配置这麻烦事了
    # entrypoint 是在setup.py中定义的（不能确定）
    for entry_point in pkg_resources.iter_entry_points(group):
        obj = entry_point.load()
        if inspect.isclass(obj):
            cmds[entry_point.name] = obj()
        else:
            raise Exception(f"Invalid entry point {entry_point.name}")
    return cmds


def _get_commands_dict(settings, inproject):
    """ 从上面的两个方法中拿到所有的模块，同时如果setting
    中有 'COMMANDS_MODULE' 在通过方法把这些模块键入到模块列表中"""
    cmds = _get_commands_from_module('scrapy.commands', inproject)
    cmds.update(_get_commands_from_entry_points(inproject))
    cmds_module = settings['COMMANDS_MODULE']
    if cmds_module:
        cmds.update(_get_commands_from_module(cmds_module, inproject))
    return cmds


def _pop_command_name(argv):
    i = 0
    for arg in argv[1:]:
        if not arg.startswith('-'):
            del argv[i]
            return arg
        i += 1


def _print_header(settings, inproject):
    version = scrapy.__version__
    if inproject:
        print(f"Scrapy {version} - project: {settings['BOT_NAME']}\n")
    else:
        print(f"Scrapy {version} - no active project\n")

#将对应所有的方法打印出来
def _print_commands(settings, inproject):
    _print_header(settings, inproject)
    print("Usage:")
    print("  scrapy <command> [options] [args]\n")
    print("Available commands:")
    cmds = _get_commands_dict(settings, inproject)
    for cmdname, cmdclass in sorted(cmds.items()):
        print(f"  {cmdname:<13} {cmdclass.short_desc()}")
    if not inproject:
        print()
        print("  [ more ]      More commands available when run from project directory")
    print()
    print('Use "scrapy <command> -h" to see more info about a command')


def _print_unknown_command(settings, cmdname, inproject):
    _print_header(settings, inproject)
    print(f"Unknown command: {cmdname}\n")
    print('Use "scrapy" to see available commands')

#调用函数 出错的话 用parser 传递相应问题
def _run_print_help(parser, func, *a, **kw):
    try:
        func(*a, **kw)
    except UsageError as e:
        if str(e):
            parser.error(str(e))
        if e.print_help:
            parser.print_help()
        sys.exit(2)

# *核心*
def execute(argv=None, settings=None):
    # 是否用其他方式传入命令参数，否的话使用命令行参数
    if argv is None:
        argv = sys.argv

    if settings is None:
        # 没指定setting的话，就调用默认方法
        settings = get_project_settings()
        # set EDITOR from environment if available 用于编辑文件
        try:
            editor = os.environ['EDITOR']
        except KeyError:
            pass
        else:
            settings['EDITOR'] = editor

    inproject = inside_project()#判断是否在项目内
    cmds = _get_commands_dict(settings, inproject) #拿到当前状态下所有可用模块
    cmdname = _pop_command_name(argv) #从命令行里拿到指向模块那个
    parser = optparse.OptionParser(formatter=optparse.TitledHelpFormatter(),
                                   conflict_handler='resolve')
    #指定一个 optparse 解析器
    if not cmdname: #未解析出指向模块
        _print_commands(settings, inproject)
        sys.exit(0)
    elif cmdname not in cmds: #解析出的字符不在可用模块内
        _print_unknown_command(settings, cmdname, inproject)
        sys.exit(2)

    cmd = cmds[cmdname] #拿到模块
    ## 解析命令
    parser.usage = f"scrapy {cmdname} {cmd.syntax()}"
    parser.description = cmd.long_desc()
    settings.setdict(cmd.default_settings, priority='command')
    cmd.settings = settings
    cmd.add_options(parser)
    opts, args = parser.parse_args(args=argv[1:])
    ## 解析命令结束

    #运行命令
    _run_print_help(parser, cmd.process_options, args, opts)
    # 生成CrawlerProcess
    cmd.crawler_process = CrawlerProcess(settings)
    # 将命令的crawler_process 再次运行（）？？？
    _run_print_help(parser, _run_command, cmd, args, opts)
    sys.exit(cmd.exitcode)


def _run_command(cmd, args, opts):
    if opts.profile:
        _run_command_profiled(cmd, args, opts)
    else:
        cmd.run(args, opts)

# 目前还不太理解 ！！！！
def _run_command_profiled(cmd, args, opts):
    if opts.profile:
        sys.stderr.write(f"scrapy: writing cProfile stats to {opts.profile!r}\n")
    loc = locals()
    p = cProfile.Profile()
    p.runctx('cmd.run(args, opts)', globals(), loc)
    if opts.profile:
        p.dump_stats(opts.profile)


if __name__ == '__main__':
    try:
        execute()
    finally:
        # Twisted prints errors in DebugInfo.__del__, but PyPy does not run gc.collect() on exit:
        # http://doc.pypy.org/en/latest/cpython_differences.html
        # ?highlight=gc.collect#differences-related-to-garbage-collection-strategies
        garbage_collect()
