# -*- coding: utf-8 -*-
'''
Created on 2017/3/16.
@author: love
'''
import time
import gevent
from gevent.queue import Queue, Empty
from gevent.event import Event
from amqp_server import AMQPRPCServer
from local_server import LocalRPCServer
from pymqant.rpc.public import ResultInfo,CallInfo,Bytes
import pymqant.rpc.public as public
class FunctionInfo():
    def __init__(self,function,goroutine):
        self.function=function
        self.goroutine=goroutine

class RPCServer():
    def __init__(self):
        self.functions={}
        self.remote_server=None
        self.local_server=None
        self.recv_queue    =Queue()  #接收到请求信息的队列
        self.result_queue    =Queue()  #接收到请求信息的队列
        self.handle_stoping=False

        self.recv_stop_evt=Event()
        self.result_stop_evt=Event()


        self.recv_greenlet=None
        self.result_greenlet=None
        self.NewLocalRPCServer()
        #启动发送消息的协程
        self.recv_greenlet=gevent.spawn(self.recv_call_handle)
        self.recv_greenlet.start()
        self.result_greenlet=gevent.spawn(self.result_call_handle)
        self.result_greenlet.start()

    def NewRemoteRPCServer(self,amqp_info):
        self.remote_server=AMQPRPCServer(amqp_info,self.recv_queue)
        self.remote_server.start()

    def NewLocalRPCServer(self):
        self.local_server=LocalRPCServer(self.recv_queue)
        self.local_server.start()

    def get_local_server(self):
        return self.local_server

    def Register(self,fname, func):
        if self.functions.has_key(fname):
            raise Exception("function name %s: already registered"%fname, 1)
        self.functions[fname]=FunctionInfo(func,False)

    def RegisterGO(self,fname, func):
        if self.functions.has_key(fname):
            raise Exception("function name %s: already registered"%fname, 1)
        self.functions[fname]=FunctionInfo(func,True)

    def recv_call_handle(self):
        while   True:
            if self.recv_queue.empty()&self.handle_stoping:
                self.recv_stop_evt.set()
                return
            if not self.recv_queue.empty():
                callInfo=self.recv_queue.get_nowait()
                if callInfo.Expired < (int(round(time.time() * 1000))) :
                    print("timeout: This is Call", callInfo.Fn, callInfo.Expired, int(round(time.time() * 1000)))
                    continue

                self.runFunc(callInfo)
            gevent.sleep(0)

    def result_call_handle(self):
        while   True:
            if self.result_queue.empty()&self.handle_stoping:
                self.result_stop_evt.set()
                return
            if not self.result_queue.empty():
                callInfo=self.result_queue.get_nowait()
                if callInfo.Reply :
                    #需要回复的才回复
                    callInfo.agent.Callback(callInfo)
            gevent.sleep(0)

    def runFunc(self,callInfo):
        if not self.functions.has_key(callInfo.Fn):
            rinfo=ResultInfo(callInfo.Cid,None,"Remote function(%s) not found"%callInfo.Fn)
            callInfo.Result=rinfo
            self.result_queue.put(callInfo)
            return
        functionInfo=self.functions[callInfo.Fn]
        _func = functionInfo.function
        params = callInfo.Args
        ArgsType=callInfo.ArgsType
        if len(params)!=len(ArgsType):
            rinfo=ResultInfo(callInfo.Cid,None,str("The number of params %s is not adapted ArgsType .%s" %(params, callInfo.ArgsType)))
            callInfo.Result=rinfo
            self.result_queue.put(callInfo)
            return
        index=0
        for type in ArgsType:
            if type==public.BYTES:
                try:
                    base64str=params[index]
                    params[index]=Bytes.b64decode(base64str)
                except Exception, err:
                    rinfo=ResultInfo(callInfo.Cid,None,str(err))
                    callInfo.Result=rinfo
                    self.result_queue.put(callInfo)
                    return
            index+=1
        def _runFunc():
            try:
                Result,Error=_func(*params)
                rinfo=ResultInfo(callInfo.Cid,Result,Error)
                callInfo.Result=rinfo
                self.result_queue.put(callInfo)
                return
            except Exception, err:
                rinfo=ResultInfo(callInfo.Cid,None,str(err))
                callInfo.Result=rinfo
                self.result_queue.put(callInfo)
                return
            finally:
                pass
                #print "Error: 没有找到文件或读取文件失败"

        if functionInfo.goroutine:
            gevent.spawn(_runFunc).start()
        else:
            _runFunc()

    def shutdown(self):
        self.handle_stoping=True      #给发送消息的协程退出指令
        if self.recv_greenlet!=None:
            self.recv_stop_evt.wait() #等待发送消息的协程退出
        if self.result_greenlet!=None:
            self.result_stop_evt.wait() #等待发送消息的协程退出
        if self.remote_server!=None:
            self.remote_server.shutdown()
        if self.local_server!=None:
            self.local_server.shutdown()
