# -*- coding: utf-8 -*-
'''
Created on 2017/3/16.
@author: love
'''

from gevent.queue import Queue, Empty
from pymqant.rpc.amqp_client import AMQPRPCClient
from pymqant.rpc.local_client import LocalRPCClient
from pymqant.rpc.local_server import LocalRPCServer
from pymqant.rpc.public import CallInfo,Bytes
import pymqant.rpc.public as public
class RPCClient():
    def __init__(self):
        self.remote_client=None
        self.local_client=None
        self.stoping=False
    def NewRemoteClient(self,amqp_info):
        self.remote_client=AMQPRPCClient(amqp_info)
        self.remote_client.start()

    def NewLocalClient(self,localRPCServer):
        self.local_client=LocalRPCClient(localRPCServer)
        self.local_client.start()

    def Call(self,_func, *params):
        if self.stoping:
            return None,"Process ready to stop!"
        ArgsType=[]
        index=0
        args=[]
        for arg in params:
            if arg==None:
                return None,"Parameter cannot be None!"
            elif isinstance(arg,(str,unicode)):
                ArgsType.append(public.STRING)
                args.append(arg)
            elif isinstance(arg,long):
                ArgsType.append(public.LONG)
                args.append(arg)
            elif isinstance(arg,float):
                ArgsType.append(public.DOUBLE)
                args.append(arg)
            elif isinstance(arg,bool):
                ArgsType.append(public.BOOL)
                args.append(arg)
            elif isinstance(arg,int):
                #int判断要放在bool 后面,否则布尔类型会被判断为int
                ArgsType.append(public.INT)
                args.append(arg)
            elif isinstance(arg,Bytes):
                ArgsType.append(public.BYTES)
                #转base64
                args.append(arg.b64encode())
            elif isinstance(arg,dict):
                ArgsType.append(public.MAP)
                args.append(arg)
            else:
                return None,"args[%d] [%s] Types not allowed"%(index,type(arg))
            index+=1

        callInfo=CallInfo(_func,args,ArgsType,timeout=5)
        callback=Queue()
        if self.local_client!=None:
            err=self.local_client.Call(callInfo,callback)
        elif self.remote_client!=None:
            err=self.remote_client.Call(callInfo,callback)

        else:
            return None, "rpc service connection failed"

        if err!=None:
            return None,err
        try:
            resultInfo=callback.get()
            return resultInfo.Result, resultInfo.Error
        except Empty:
            return None,"Quitting time!"



    def CallNR(self,_func, *params):
        if self.stoping:
            return None,"Process ready to stop!"
        ArgsType=[]
        index=0
        args=[]
        for arg in params:
            if arg==None:
                return  None,"Parameter cannot be None!"
            elif isinstance(arg,(str,unicode)):
                ArgsType.append(public.STRING)
                args.append(arg)
            elif isinstance(arg,long):
                ArgsType.append(public.LONG)
                args.append(arg)
            elif isinstance(arg,float):
                ArgsType.append(public.DOUBLE)
                args.append(arg)
            elif isinstance(arg,bool):
                ArgsType.append(public.BOOL)
                args.append(arg)
            elif isinstance(arg,int):
                #int判断要放在bool 后面,否则布尔类型会被判断为int
                ArgsType.append(public.INT)
                args.append(arg)
            elif isinstance(arg,Bytes):
                ArgsType.append(public.BYTES)
                #转base64
                args.append(arg.b64encode())
            elif isinstance(arg,dict):
                ArgsType.append(public.MAP)
                args.append(arg)
            else:
                return None,"args[%d] [%s] Types not allowed"%(index,type(arg))
            index+=1


        callInfo=CallInfo(_func,args,ArgsType,timeout=5)
        if self.local_client!=None:
            err=self.local_client.CallNR(callInfo)
        elif self.remote_client!=None:
            err=self.remote_client.CallNR(callInfo)
        else:
            return "rpc service connection failed"
        return err

    def shutdown(self):
        self.stoping=True
        if self.remote_client!=None:
            self.remote_client.shutdown()
        if self.local_client!=None:
            self.local_client.shutdown()


