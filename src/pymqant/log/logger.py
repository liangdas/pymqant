# -*- coding: utf-8 -*-
'''
Created on 2017/3/19.
@author: love
'''
import os
import logging
from logging.handlers import RotatingFileHandler
log=logging.getLogger("pymqant")
log.setLevel(logging.DEBUG)
def init_log_config(debug,ProcessID,logdir):
    if  debug==False:
        # nohup=logdir+os.sep+'%s.nohup.log'%ProcessID
        # logging.basicConfig(level=logging.NOTSET,
        #                     format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
        #                     datefmt='%Y-%m-%d %H:%M:%S',
        #                     filename=nohup,
        #                     filemode='w'
        #                     )
        #################################################################################################
        #定义一个RotatingFileHandler，最多备份5个日志文件，每个日志文件最大10M
        Rthandlererror = RotatingFileHandler(logdir+os.sep+'%s.error.log'%ProcessID, maxBytes=100*1024*1024,backupCount=5)
        Rthandlererror.setLevel(logging.WARN)
        formatter = logging.Formatter('%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s')
        Rthandlererror.setFormatter(formatter)
        log.addHandler(Rthandlererror)

        #################################################################################################
        #定义一个RotatingFileHandler，最多备份5个日志文件，每个日志文件最大10M
        Rthandler = RotatingFileHandler(logdir+os.sep+'%s.access.log'%ProcessID, maxBytes=100*1024*1024,backupCount=5)
        Rthandler.setLevel(logging.INFO)
        formatter = logging.Formatter('%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s')
        Rthandler.setFormatter(formatter)
        log.addHandler(Rthandler)

    else:
        # logging.basicConfig(level=logging.DEBUG,
        #                     format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
        #                     datefmt='%Y-%m-%d %H:%M:%S',
        #                     )
        #################################################################################################
        #定义一个StreamHandler，将INFO级别或更高的日志信息打印到标准错误，并将其添加到当前的日志处理对象#
        console = logging.StreamHandler()
        console.setLevel(logging.NOTSET)
        formatter = logging.Formatter('%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s')
        console.setFormatter(formatter)
        log.addHandler(console)
        #################################################################################################
