# -*- coding: utf-8 -*-
'''
Created on 2017/3/19.
@author: love
'''
import sys,os
import binascii
import math
import signal
import json
import pymqant
from optparse import OptionParser
from gevent.event import Event
from pymqant.log.logger import init_log_config,log
from ServerSession import ServerSession
from pymqant.rpc.client import RPCClient
from module import ModuleManager
#获取脚本文件的当前路径
def cur_file_dir():
    #获取脚本路径
    path = sys.path[0]
    #判断为脚本文件还是py2exe编译后的文件，如果是脚本文件，则返回的是脚本的目录，如果是py2exe编译后的文件，则返回的是编译后的文件路径
    if os.path.isdir(path):
        return path
    elif os.path.isfile(path):
        return os.path.dirname(path)
class   mqant():
    def __init__(self):
        self.routes={}
        self.defaultRoutes=self._default_routes
        self.serverList={}
        self.settings=None
        self.sig=-1;
    def _default_routes(self,app, Type, hash):
        servers = app.GetServersByType(Type)
        if len(servers) == 0 :
            return None
        index = int(abs(binascii.crc32(hash)) % len(servers))
        return servers[index]
    def Run(self,debug,*modules):
        usage = "usage: %prog [options] arg"
        parser = OptionParser(usage)
        parser.add_option("--conf", default=os.getcwd()+os.sep+"conf/server.conf",
                          help="Server configuration file path",dest="conf")
        parser.add_option("--pid",default="development",
                          help="Server ProcessID?", dest="pid")
        parser.add_option("--log",default=os.getcwd()+os.sep+"logs",
                          help="Log file directory?", dest="log")
        (options, args) = parser.parse_args()
        # if len(args) != 1:
        #     parser.error("incorrect number of arguments")
        ProcessID=options.pid
        init_log_config(debug,ProcessID,options.log)
        log.info("Server configuration file path [%s]" % options.conf)
        log.info("pymqant %s starting up", pymqant.version)
        f=open(options.conf,"r")
        strings=[]
        list_of_all_the_lines = f.readlines( )
        for line in list_of_all_the_lines:
            line=line.strip()
            if line.startswith("//")==False:
                strings.append(line)
        f.close()
        settings = json.loads("".join(strings))
        manager=ModuleManager()
        for mod in modules:
            manager.Register(mod)
        self.OnInit(settings)
        manager.Init(self, ProcessID)

        stop=Event()
        def signal_handler(signum, frame):
            exit()
            log.error("Forced exit process!")
        def handler(signum, frame):
            self.sig=signum
            stop.set()
            signal.signal(signal.SIGALRM, signal_handler) # 14
            signal.alarm(5) #5秒后强制退出
        signal.signal(signal.SIGINT, handler)
        signal.signal(signal.SIGTERM, handler)
        stop.wait()
        manager.Destroy()
        self.OnDestroy()
        log.info("pymqant closing down (signal: %d)", self.sig)

    def OnInit(self, settings):
        self.settings=settings
        modules=settings["Module"]
        for Type, ModuleInfos in modules.items():
            for moduel in ModuleInfos:
                if self.serverList.has_key(moduel["Id"]):
                    #如果Id已经存在,说明有两个相同Id的模块,这种情况不能被允许,这里就直接抛异常 强制崩溃以免以后调试找不到问题
                    m = self.serverList[moduel["Id"]]
                    raise Exception("ServerId (%s) Type (%s) of the modules already exist Can not be reused ServerId (%s) Type (%s)"% (m.Id, m.Stype, moduel["Id"], Type))
                client=RPCClient()
                if moduel.has_key("Rabbitmq"):
                    #如果远程的rpc存在则创建一个对应的客户端
                    client.NewRemoteClient(moduel["Rabbitmq"])
                session=ServerSession(moduel["Id"],Type,client)
                self.serverList[moduel["Id"]] = session
                log.info("RPCClient create success type(%s) id(%s)"%(Type, moduel["Id"]))
    def OnDestroy(self):
        for id,session in self.serverList.items():
            err=session.Rpc.shutdown()
            if err!=None:
                log.warning("RPCClient close fail type(%s) id(%s)", session.Stype, id)
            else:
                log.info("RPCClient close success type(%s) id(%s)", session.Stype, id)

        return None

    def Route(self,moduleType, fn) :
        '''
            当同一个类型的Module存在多个服务时,需要根据情况选择最终路由到哪一个服务去
            fn: func(moduleType,serverId,[]*ServerSession)(*ServerSession)
        '''
        self.routes[moduleType] = fn
        return None
    def _get_route(self,moduleType):
        if self.routes.has_key(moduleType):
            return self.routes[moduleType]
        else:
            return self.defaultRoutes
    def RegisterLocalClient(self,serverId, server) :
        if self.serverList.has_key(serverId):
            session=self.serverList[serverId]
            return session.Rpc.NewLocalClient(server)
        else:
            return "Server(%s) Not Found"%serverId
        return None
    def GetServersById(self,serverId):
        if self.serverList.has_key(serverId):
            session=self.serverList[serverId]
            return session,None
        else:
            return None, "Server(%s) Not Found"%serverId

    def GetRouteServers(self,filter, hash) :
        '''
        filter		 调用者服务类型    moduleType|moduleType@moduleID
        Type	   	想要调用的服务类型
        '''
        sl = filter.split("@")
        if len(sl) == 2 :
            moduleID = sl[1]
            if moduleID != None :
                return self.GetServersById(moduleID)

        moduleType = sl[0]
        route = self._get_route(moduleType)
        s = route(self, moduleType, hash)
        if s == None:
            return None,"Server(type : %s) Not Found"%moduleType
        else:
            return s,None
    def GetServersByType(self,Type):
        servers=[]
        for id,server in self.serverList.items():
            if server.Stype==Type:
                servers.append(server)
        return servers
    def GetSettings(self):
        '''
        获取配置信息
        '''
        return self.settings
    def RpcInvoke(self,module, moduleType, _func, *params):
        server, e = self.GetRouteServers(moduleType, module.GetServerId())
        if e != None:
            return  None,e
        return server.Call(_func, *params)
    def RpcInvokeNR(self,module, moduleType, _func, *params) :
        server, e = self.GetRouteServers(moduleType, module.GetServerId())
        if e != None:
            return  e
        return server.CallNR(_func, *params)

