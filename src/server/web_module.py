# -*- coding: utf-8 -*-
'''
Created on 2017/3/21.
@author: love
'''
from flask import Flask
import gevent
from pymqant.log.logger import log
from pymqant.module.module import RPCModule
from pymqant.rpc.public import Bytes
import struct
flask = Flask(__name__)
@flask.route('/')
def hello_world():
    return 'Hello World!'
class WebModule(RPCModule):
    def __init__(self):
        RPCModule.__init__(self)
        self.app=None
        self.flask=None
    def GetType(self):
        return "Test"

    def OnInit(self,app,settings):
        self.app=app
    def Run(self):
        flask.run()
    def OnDestroy(self):
        print "模块退出"