'''
生成日志文件
'''
import datetime
import logging
import os
import re
from logging.handlers import TimedRotatingFileHandler
from time import sleep



# def get_log():
#     filename = '%s.log' % datetime.datetime.strftime(
#         datetime.datetime.now(), '%Y-%m-%d')
#     log_file = './deal_log_file/' + filename
#     if not os.path.isdir('deal_log_file'):
#         os.makedirs('deal_log_file')
#     if not os.path.isfile(log_file):  # 无文件时创建
#         fd = open(log_file, mode="w", encoding="utf-8")
#         fd.close()
#     else:
#         pass
#
#     log_format = '%(asctime)s[%(levelname)s]: %(message)s'
#     logging.basicConfig(
#         filename=log_file,
#         level=logging.DEBUG,
#         format=log_format)
#     logger = logging.getLogger()
#     return logger, filename


def get_log():
    logging.basicConfig()
    log = logging.getLogger()
    log.setLevel(logging.INFO)

    if not os.path.isdir('deal_log_file'):
        os.makedirs('deal_log_file')
    file_path = get_other_path('./deal_log_file/log.txt')
    # 保留日志文件数量5
    filehander = logging.handlers.TimedRotatingFileHandler(file_path, when='H', interval=24, backupCount=5)
    filehander.suffix = '%Y-%m-%d_%H-%M-%S.log'
    filehander.extMatch = re.compile(r"^\d{4}-\d{2}-\d{2}_\d{2}-\d{2}-\d{2}.log$")
    filehander.extMatch = re.compile(filehander.extMatch)

    log_format = '%(asctime)s[%(levelname)s]: %(message)s'
    fmt = logging.Formatter(log_format)
    filehander.setFormatter(fmt)
    log.addHandler(filehander)
    return log


def get_root_path():
    curpath = os.path.abspath(os.path.dirname(__file__))
    rootPath = curpath[:curpath.find("TOBT") + len("TOBT")]
    return rootPath


def get_other_path(abspath):
    rootPath = get_root_path()
    datapath = os.path.abspath(rootPath + abspath)
    return datapath
