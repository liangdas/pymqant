# -*- coding: utf-8 -*-
'''
Created on 17/3/13.
@author: love
'''
import gevent
import gevent.monkey
gevent.monkey.patch_all()
from pymqant.module.app import mqant
from server.chat_module import ChatModule
from server.test_module import TestModule

if __name__ == "__main__":
    app=mqant()
    app.Run(True,ChatModule(),TestModule())