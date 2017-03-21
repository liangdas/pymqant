# -*- coding: utf-8 -*-
'''
Created on 2017/3/19.
@author: love
'''
class ServerSession():
    def __init__(self,Id,Stype,Rpc):
        self.Id=Id
        self.Stype=Stype
        self.Rpc=Rpc
    def Call(self,_func, *params):
        return self.Rpc.Call(_func,*params)

    def CallNR(self,_func, *params):
        return self.Rpc.CallNR(_func,*params)
