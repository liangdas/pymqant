# -*- coding: utf-8 -*-
'''
Created on 17/3/13.
@author: love
'''
import time
import gevent
from gevent import Greenlet
from gevent.queue import Queue, Empty
from pymqant.log.logger import log
from gevent.coros import BoundedSemaphore
from gevent.event import Event
from pymqant.rpc.public import ResultInfo
class  ClinetCallInfo () :
    def __init__(self,correlation_id,callInfo,call):
        self.correlation_id=correlation_id #
        self.call=call              #
        self.callInfo=callInfo
        self.timeout=callInfo.Expired


class LocalRPCClient(Greenlet):
    def __init__(self,localServer):
        Greenlet.__init__(self)
        self.callinfos={}
        self.localServer=localServer
        self.recv_queue=Queue()
        self.lock = BoundedSemaphore(1)


        self.handle_stoping=False
        self.recv_stop_evt=Event()
        self.timeout_stop_evt=Event()

        self.timeout_handle_greenlet=gevent.spawn(self.on_timeout_handle)
        self.timeout_handle_greenlet.start()
    def on_response_handle(self):
        while   True:
            if self.recv_queue.empty()&self.handle_stoping:
                self.recv_stop_evt.set()
                return
            if not self.recv_queue.empty():
                callinfo=self.recv_queue.get_nowait()
                self.lock.acquire()
                Cid=callinfo["Cid"]
                if self.callinfos.has_key(Cid):
                    ci=self.callinfos[Cid]
                    if ci.call!=None:
                        ci.call.put(callinfo)
                    else:
                        log.warning("AMQPRPCClient : Received a message, but did not set the callback function")
                    del self.callinfos[Cid]
                self.lock.release()
            gevent.sleep(0)

    def on_timeout_handle(self):
        while   True:
            if self.handle_stoping:
                self.lock.acquire()
                #给所有发送停止消息
                for cid,clinetCallInfo in self.callinfos.items():
                    if clinetCallInfo != None:
                        #已经超时了
                        resultInfo=ResultInfo(clinetCallInfo.correlation_id,None,"Process ready to stop")

                        clinetCallInfo.call.put(resultInfo)
                        #从Map中删除
                        del self.callinfos[clinetCallInfo.correlation_id]
                self.lock.release()
                self.timeout_stop_evt.set()
                return
            self.lock.acquire()
            for cid,clinetCallInfo in self.callinfos.items():
                if clinetCallInfo != None:
                    if clinetCallInfo.timeout < int(round(time.time() * 1000)):
                        #已经超时了
                        resultInfo=ResultInfo(clinetCallInfo.correlation_id,None,"timeout: This is Call")

                        clinetCallInfo.call.put(resultInfo)
                        #从Map中删除
                        del self.callinfos[clinetCallInfo.correlation_id]
            self.lock.release()
            gevent.sleep(1)


    # 发出RPC请求
    def Call(self, callInfo,callback):
        self.lock.acquire()
        callInfo.props["reply_to"]=callback
        ccf=ClinetCallInfo(callInfo.Cid,callInfo,callback)
        self.callinfos[callInfo.Cid]=ccf
        self.lock.release()
        # 发送RPC请求内容到RPC请求队列`rpc_queue`，同时发送的还有`reply_to`和`correlation_id`
        self.localServer.write(callInfo)
        return None

    # 发出RPC请求
    def CallNR(self, callInfo):
        ccf=ClinetCallInfo(callInfo.Cid,callInfo,None)
        self.callinfos[callInfo.Cid]=ccf
        self.lock.release()
        # 发送RPC请求内容到RPC请求队列`rpc_queue`，同时发送的还有`reply_to`和`correlation_id`
        self.localServer.write(callInfo)
        return None


    def _run(self):
        try:
            self.on_response_handle()
        except Exception,e:
            print e

    def shutdown(self):
        self.handle_stoping=True
        if self.recv_queue!=None:
            self.recv_stop_evt.wait() #等待发送消息的协程退出
        if self.timeout_handle_greenlet!=None:
            self.timeout_stop_evt.wait()







