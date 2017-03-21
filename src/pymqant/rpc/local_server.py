# -*- coding: utf-8 -*-
'''
Created on 2017/3/19.
@author: love
'''
import gevent
from gevent.queue import Queue, Empty
from gevent.event import Event
from gevent import Greenlet
class   LocalRPCServer(Greenlet):

    def __init__(self,server_queue):
        Greenlet.__init__(self)
        self.server_queue=server_queue
        self.recv_queue=Queue()

        self.handle_stoping=False

        self.recv_stop_evt=Event()
    def write(self,callInfo):
        self.recv_queue.put(callInfo)

    # 对RPC请求队列中的请求进行处理
    def recv_call_handle(self):
        while   True:
            if self.recv_queue.empty()&self.handle_stoping:
                self.recv_stop_evt.set()
                return
            if not self.recv_queue.empty():
                callInfo=self.recv_queue.get_nowait()
                callInfo.agent=self
                self.server_queue.put(callInfo)
            gevent.sleep(0)



    def Callback(self,callInfo):
        reply_to = callInfo.props["reply_to"]
        reply_to.put(callInfo.Result)

    def _run(self):
        try:
            self.recv_call_handle()
        except Exception,e:
            print e


    def shutdown(self):
        if self.handle_stoping==False:
            self.handle_stoping=True      #给发送消息的协程退出指令
            self.recv_stop_evt.wait() #等待发送消息的协程退出
