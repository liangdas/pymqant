# -*- coding: utf-8 -*-
'''
Created on 2017/3/19.
@author: love
'''
import gevent
from pymqant.log.logger import log
from pymqant.module.module import RPCModule
from pymqant.rpc.public import Bytes
import struct
class TestModule(RPCModule):
    def __init__(self):
        RPCModule.__init__(self)
        self.app=None
        self.closeSig=False
    def GetType(self):
        return "Test"
    def OnInit(self,app,settings):
        self.app=app
    def stop(self):
        self.closeSig=True
    def Run(self):
        while  self.closeSig==False:
            gevent.sleep(1)
            log.debug(" [x] Requesting fib(30)")
            values = (1, 'abc', 2.7)
            s = struct.Struct('I3sf')
            packed_data = s.pack(*values)
            response,err =self.RpcInvoke("Login","getRand",Bytes("hallo"),{"name":"liangdas"},1.001,50,True)
            if err!="":
                log.debug(" [.] 错误 %r" % err)
            else:
                log.debug(" [.] 结果 %r" % response)
    def OnDestroy(self):
        pass