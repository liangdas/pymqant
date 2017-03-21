# -*- coding: utf-8 -*-
'''
Created on 17/3/13.
@author: love
'''
#!/usr/bin/env python
import json
from gevent import Greenlet
from consumer import ExampleConsumer
from pymqant.rpc.public import ResultInfo,CallInfo


class   AMQPRPCServer(Greenlet,ExampleConsumer):

    def __init__(self,amqp_info,recv_queue):
        Greenlet.__init__(self)
        ExampleConsumer.__init__(self,amqp_info)
        self.recv_queue=recv_queue


    # 对RPC请求队列中的请求进行处理
    def on_message(self,ch, method, props, body):
        ch.basic_ack(delivery_tag = method.delivery_tag)
        callInfo= json.loads(body)
        callInfo["agent"]=self
        callInfo["props"]=props
        ci=CallInfo()
        ci.fromDict(callInfo)
        self.recv_queue.put(ci)


    def Callback(self,callInfo):
        result=callInfo.Result
        props=callInfo.props
        d = {}
        d.update(result.__dict__)
        body= json.dumps(d)
        #调用数据处理方法
        #将处理结果(响应)发送到回调队列
        self._channel.basic_publish(exchange="",
                         routing_key=props.reply_to,
                         body=body)

    def _run(self):
        # # 负载均衡，同一时刻发送给该服务器的请求不超过一个
        # self.channel.basic_qos(prefetch_count=1)
        try:
            self.rabbitmq_run()
        except KeyboardInterrupt:
            self.rabbitmq_stop()

    def shutdown(self):
        self.rabbitmq_stop()
