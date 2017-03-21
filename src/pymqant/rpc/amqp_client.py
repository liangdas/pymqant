# -*- coding: utf-8 -*-
'''
Created on 17/3/13.
@author: love
'''
import pika
import uuid
import gevent
import time
import gevent
import json
from pymqant.rpc.public import ResultInfo
from pymqant.log.logger import log
from gevent import Greenlet
from gevent.queue import Queue, Empty
from gevent.coros import BoundedSemaphore
from consumer import ExampleConsumer
from gevent.event import Event
class  ClinetCallInfo () :
    def __init__(self,correlation_id,body,call,timeout):
        self.correlation_id=correlation_id #
        self.call=call              #
        self.body=body
        self.timeout=timeout


class AMQPRPCClient(Greenlet,ExampleConsumer):
    def __init__(self,amqp_info):
        Greenlet.__init__(self)
        ExampleConsumer.__init__(self,amqp_info)
        self.callinfos={}
        self.send_queue=Queue()
        self.lock = BoundedSemaphore(1)
        self.send_greenlet=None


        self.handle_stoping=False
        self.send_stop_evt=Event()

        self.timeout_stop_evt=Event()

        self.timeout_handle_greenlet=gevent.spawn(self.on_timeout_handle)
        self.timeout_handle_greenlet.start()
    def send_task(self):
        while   True:
            if self.send_queue.empty()&self.handle_stoping:
                self.send_stop_evt.set()
                return
            if not self.send_queue.empty():
                callinfo=self.send_queue.get_nowait()
                # 发送RPC请求内容到RPC请求队列`rpc_queue`，同时发送的还有`reply_to`和`correlation_id`
                self._channel.basic_publish(exchange=self.Exchange,
                                            routing_key=self.Queue,
                                            properties=pika.BasicProperties(
                                                    reply_to = self.callback_queue,
                                            ),
                                            body=callinfo.body)

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

    # 对回调队列中的响应进行处理的函数
    def on_message(self, ch, method, props, body):
        self.lock.acquire()
        result=json.loads(body)
        for key in self.callinfos:
            if key == result["Cid"]:
                callInfo=self.callinfos[key]
                if callInfo.call!=None:
                    ri=ResultInfo()
                    ri.fromDict(result)
                    callInfo.call.put(ri)
                else:
                    log.warning("AMQPRPCClient : Received a message, but did not set the callback function")
                del self.callinfos[key]
                break
        self.lock.release()

    # 发出RPC请求
    def Call(self, callInfo,callback):
        self.lock.acquire()
        callInfo.ReplyTo=self.callback_queue
        d = {}
        d.update(callInfo.__dict__)
        body = json.dumps(d)
        ccf=ClinetCallInfo(callInfo.Cid,body,callback,callInfo.Expired)
        self.callinfos[callInfo.Cid]=ccf
        self.lock.release()
        # 发送RPC请求内容到RPC请求队列`rpc_queue`，同时发送的还有`reply_to`和`correlation_id`
        self.send_queue.put(ccf)
        return None

    # 发出RPC请求
    def CallNR(self, callInfo):
        self.lock.acquire()
        body = json.dumps(callInfo.__dict__)
        ccf=ClinetCallInfo(callInfo.Cid,body,None,callInfo.Expired)
        self.callinfos[callInfo.Cid]=ccf
        self.lock.release()
        # 发送RPC请求内容到RPC请求队列`rpc_queue`，同时发送的还有`reply_to`和`correlation_id`
        self.send_queue.put(ccf)
        return None


    def _run(self):
        try:
            self.rabbitmq_run()
        except KeyboardInterrupt:
            self.rabbitmq_stop()

    def shutdown(self):
        self.handle_stoping=True #给发送消息的协程退出指令
        if self.send_greenlet!=None:
            self.send_stop_evt.wait() #等待发送消息的协程退出
        if self.timeout_handle_greenlet!=None:
            self.timeout_stop_evt.wait()
        self.rabbitmq_stop()

    def on_channel_open(self, channel):
        self._channel = channel
        self._channel.add_on_close_callback(self._on_channel_closed)
        self._channel.queue_declare(self.on_queue_declareok,"", exclusive=True,auto_delete=True)

    def on_queue_declareok(self, method_frame):
        self.callback_queue=method_frame.method.queue
        self.start_consuming()

    def on_bindok(self, unused_frame):
        self.start_consuming()

    def start_consuming(self):
        self._channel.add_on_cancel_callback(self._on_consumer_cancelled)
        self._consumer_tag = self._channel.basic_consume(self.on_message,no_ack=True,
                                                         queue=self.callback_queue)

        #启动发送消息的协程
        self.send_greenlet=gevent.spawn(self.send_task)
        self.send_greenlet.start()




