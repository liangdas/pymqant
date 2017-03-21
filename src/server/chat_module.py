# -*- coding: utf-8 -*-
'''
Created on 17/3/13.
@author: love
'''
from pymqant.module.module import RPCModule
class ChatModule(RPCModule):
    def __init__(self):
        RPCModule.__init__(self)
    def GetType(self):
        return "PyChat"
    def OnInit(self,app,settings):
        RPCModule.OnInit(self,app,settings)
        self.server.RegisterGO("fib",self.my_fib)
        self.server.RegisterGO("sayhello",self.sayhello)
    def sayhello(self,name,say):
        print type(say)
        print say
        return "say ok",None
    def my_fib(self,n):
        r=self.fib(n)
        return r,None
    # 数据处理方法
    def fib(self,n):
        if n == 0:
            return 0
        elif n == 1:
            return 1
        else:
            return self.fib(n-1) + self.fib(n-2)
    def Run(self):
        pass
    def OnDestroy(self):
        RPCModule.OnDestroy(self)