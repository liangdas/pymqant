# -*- coding: utf-8 -*-
'''
Created on 17/3/13.
@author: love
'''
import gevent
from pymqant.log.logger import log
from gevent import Greenlet
from gevent.pool import Group
from gevent.event import Event
from pymqant.rpc.server import RPCServer
class Module (Greenlet) :
    def __init__(self):
        Greenlet.__init__(self)
        self._stop_evt=Event()
        self.settings=None
    def _run(self):
        try:
            self.Run()
        except Exception,e:
            print e
        self._stop_evt.set()
    def stop(self):
        self.kill()
    def sleep(self,seconds=0):
        gevent.sleep(seconds)

class RPCModule (Module) :
    def __init__(self):
        Module.__init__(self)
        self.app=None
        self.server=None
        self.settings=None
    def GetServerId(self):
        return self.settings["Id"]
    def OnInit(self,app,settings):
        self.app=app
        self.settings=settings
        self.server=RPCServer()
        if settings.has_key("Rabbitmq"):
            self.server.NewRemoteRPCServer(settings["Rabbitmq"])
        # err = app.RegisterLocalClient(settings["Id"], self.server.get_local_server())
        # if err != None :
        #     logging.warning("RegisterLocalClient: id(%s) error(%s)", settings["Id"], err)
        log.info("RPCServer init success id(%s)", settings["Id"])

    def OnDestroy(self):
        self.server.shutdown()
        log.info("RPCServer close success id(%s)", self.settings["Id"])

    def GetModuleSettings(self):
        return self.settings

    def GetRouteServers(self,moduleType, hash):
        return self.app.GetRouteServers(moduleType,hash)

    def RpcInvoke(self,moduleType, _func, *params):
        return self.app.RpcInvoke(self,moduleType,_func,*params)

    def RpcInvokeNR(self,moduleType, _func, *params):
        return self.app.RpcInvokeNR(self,moduleType,_func,*params)
class ModuleManager():
    def __init__(self):
        self.mods=[]
        self.runMods=[]
        self.group = Group()
        self.app=None
        self.ProcessID=None
    def Init(self,app,ProcessID):
        self.app=app
        self.ProcessID=ProcessID
        log.info("This service ProcessID is [%s]", ProcessID)
        self.CheckModuleSettings()
        modules=app.GetSettings()["Module"]
        for mod in self.mods :
            for Type, modSettings in modules.items():
                if mod.GetType() == Type :
                    #匹配
                    for setting in modSettings:
                        #这里可能有BUG 公网IP和局域网IP处理方式可能不一样,先不管
                        if ProcessID == setting["ProcessID"] :
                            self.runMods.append(mod) #这里加入能够运行的组件
                            mod.settings=setting
                    break #跳出内部循环
        for module in self.runMods:
            module.OnInit(app,module.settings)
            module.start()

    def CheckModuleSettings(self):
        gid = {}#用来保存全局ID-ModuleType
        modules=self.app.GetSettings()["Module"]
        for Type, modSettings in modules.items():
            pid ={} #用来保存模块中的 ProcessID-ID
            for setting in modSettings:
                if gid.has_key(setting["Id"]):
                    #如果Id已经存在,说明有两个相同Id的模块,这种情况不能被允许,这里就直接抛异常 强制崩溃以免以后调试找不到问题
                    raise Exception("ID (%s) been used in modules of type [%s] and cannot be reused"% (setting["Id"], gid[setting["Id"]]))
                else:
                    gid[setting["Id"]] = Type

                if pid.has_key(setting["ProcessID"]):
                    #如果Id已经存在,说明有两个相同Id的模块,这种情况不能被允许,这里就直接抛异常 强制崩溃以免以后调试找不到问题
                    raise Exception("In the list of modules of type [%s], ProcessID (%s) has been used for ID module for (%s)"% (Type, setting["ProcessID"], pid[setting["ProcessID"]]))
                else:
                    pid[setting["ProcessID"]] = setting["Id"]

    def Register(self,mi):
        self.mods.append(mi)
    def RegisterRun(self,mi):
        self.runMods.append(mi)

    def Destroy(self):
        for module in self.runMods:
            module.stop()
            module._stop_evt.wait() #等待协程退出
            module.OnDestroy()





