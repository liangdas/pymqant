# -*- coding: utf-8 -*-
'''
Created on 2017/3/17.
@author: love
'''
import uuid
import time
import struct
import base64
BOOL="bool"	#bool
BYTE="byte"	#byte
INT="int"	#int32
LONG="long"	#long64
FLOAT="float"	#float32
DOUBLE="double"	#float64
BYTES="bytes"	#[]byte
STRING="string" #string
MAP="map"	#map[string]interface{}
class Bytes():
    def __init__(self,bytestr=None):
        self.bytestr=bytestr
    def b64encode(self):
        return base64.urlsafe_b64encode(self.bytestr)
    @classmethod
    def b64decode(b64):
        bytestr= base64.urlsafe_b64decode(b64)
        return Bytes(bytestr)
class   ResultInfo:
    def __init__(self,Cid=None,Result=None,Error=None):
        self.Cid=Cid       #Correlation_id
        self.Error=Error     #错误结果 如果为nil表示请求正确
        self.Result=Result         #结果
    def fromDict(self,dict):
        self.Cid=dict["Cid"]
        self.Error=dict["Error"]
        self.Result=dict["Result"]
class CallInfo():
    def __init__(self,Fn=None,Args=None,ArgsType=None,Reply=True,timeout=5):
        self.Fn=Fn
        self.Args=Args
        self.ArgsType=ArgsType
        self.Reply=Reply
        self.ReplyTo=None
        self.Expired=int(round(time.time() * 1000))+timeout*1000
        self.Cid=str(uuid.uuid4())
        self.Result=None
        self.agent=None      #代理者  AMQPServer / LocalServer 都继承 Callback(callinfo CallInfo)(error) 方法
        self.props={}      #amqp属性

    def fromDict(self,dict):
        self.Fn=dict["Fn"]
        self.Args=dict["Args"]
        self.Reply=dict["Reply"]
        self.ReplyTo=dict["ReplyTo"]
        self.Expired=dict["Expired"]
        self.Cid=dict["Cid"]
        if dict["Result"]!=None:
            self.Result=ResultInfo()
            self.Result.fromDict(dict["Result"])
        else:
            self.Result=None
        self.agent=dict["agent"]
        self.props=dict["props"]