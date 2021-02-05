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
    for module in walk_modules(module_name): #这里的module_name实际上是一个路径 类似于 json.decode
        for obj in vars(module).values(): #遍历 这个模块里面所有的属性 和方法
            if (
                inspect.isclass(obj) #是类
                and issubclass(obj, ScrapyCommand) #是目标子类
                and obj.__module__ == module.__name__ # 他的名字 和它从属的类模块是相同的 （判断是模块级别）
                and not obj == ScrapyCommand #是继承后的父类
            ):
                yield obj

# 给定一个模块 和是否在项目内的flag，检查_iter_command_classes（模块）返回的方法
# 也就是检查这个函数是否需要在 项目环境中运行
def _get_commands_from_module(module, inproject):
    d = {}
    for cmd in _iter_command_classes(module):
        # 在项目内时候 或者 目标函数不需要在项目内执行时候 加入到d
        if inproject or not cmd.requires_project:#ScrapyCommand.requires_project 默认为false
            cmdname = cmd.__module__.split('.')[-1]
            d[cmdname] = cmd()
    return d

# 跟walkthrough很像，不过这里是给定一个group 返回他所有的 在group 下的 entry_point（类似于子类）
def _get_commands_from_entry_points(inproject, group='scrapy.commands'):
    cmds = {}
    # entry_point 之前的理解有错误，可以理解成系统调用这个命令 直接调用哪个函数
    # 也就是说 系统调用 scrapy 相当于直接调用 cmdline.execute()
    # entrypoint 是在setup.py中定义的
    for entry_point in pkg_resources.iter_entry_points(group):
        obj = entry_point.load() #这里相当import 引入
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
    for arg in argv[1:]: #应为sys.argv的第一个参数列表总是 这被调用的文件名 后面是通过终端输入的命令
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

#调用上面的方法 并指出不可用 CMDname
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
        argv = sys.argv #sys.argv 的第一个元素一定是本体的名字，不论后面带不带参数 所以不能用非空来判断

    if settings is None:
        # 没指定setting的话，就调用默认方法
        settings = get_project_settings()# 从scrapy.cfg载入setting 再从环境中载入scrapy相关的setting到setting对象里
        # set EDITOR from environment if available 用于编辑文件
        try:
            editor = os.environ['EDITOR']
        except KeyError:
            pass
        else:
            settings['EDITOR'] = editor

    inproject = inside_project()#判断是否在项目内(先尝试载入setting模块 否则用是否能找到scrapy.cfg来判断)
    cmds = _get_commands_dict(settings, inproject) #拿到当前状态下所有可用模块
    cmdname = _pop_command_name(argv) #从命令行里拿到指向哪个命令
    parser = optparse.OptionParser(formatter=optparse.TitledHelpFormatter(), ##指定一个 optparse 解析器 已经被argparse 替代了 不用学这个
                                   conflict_handler='resolve')

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
    #将命令中的设置弄到setting中
    settings.setdict(cmd.default_settings, priority='command')
    cmd.settings = settings
    cmd.add_options(parser)
    opts, args = parser.parse_args(args=argv[1:])
    ## 解析命令结束

    #运行命令
    _run_print_help(parser, cmd.process_options, args, opts) #调用对应的命令 传递由解析器解析出来的参数
    # 生成CrawlerProcess
    cmd.crawler_process = CrawlerProcess(settings)
    # 运行crawler_process 对应的命令
    _run_print_help(parser, _run_command, cmd, args, opts) #调用command的run启动这两个参数
    sys.exit(cmd.exitcode)


def _run_command(cmd, args, opts):
    if opts.profile:
        _run_command_profiled(cmd, args, opts)
    else:
        cmd.run(args, opts)

# 用cpython的porfiler 运行 cmd.run(args, opts) 并传入变量
def _run_command_profiled(cmd, args, opts):
    if opts.profile:
        sys.stderr.write(f"scrapy: writing cProfile stats to {opts.profile!r}\n")
    loc = locals() #拿到所有变量
    p = cProfile.Profile()
    p.runctx('cmd.run(args, opts)', globals(), loc) #调用函数 并传入 global 和 locals 中的变量
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
