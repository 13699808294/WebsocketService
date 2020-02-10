import os
import time

import tornado
from tornado.options import options
from tornado import web, httpserver, gen

from apps.websocket.websocketClient import WebsocketClient
from setting.setting import DEBUG
from utils.baseAsync import BaseAsync

tornado.options.define('websocket_service_port', type=int, default=8005, help='服务器端口号')

class WebsocketService(BaseAsync):
    def __init__(self,mosquittoClient,cerfile=None,keyfile=None):
        super().__init__()
        self.mosquittoClient = mosquittoClient
        self.clientSet = set()
        self.cerfile = cerfile
        self.keyfile = keyfile
        self.urlpatterns = [
            (r'/', WebsocketClient, {'server': self}),
        ]
        self.ioloop.add_timeout(self.ioloop.time() + 1, self.checkClientHeart)
        ssl_options = {
            'certfile': self.cerfile,
            'keyfile': self.keyfile
        }
        app = web.Application(self.urlpatterns,
                              debug=False,
                              # autoreload=True,
                              # compiled_template_cache=False,
                              # static_hash_cache=False,
                              # serve_traceback=True,
                              static_path=os.path.join(os.path.dirname(__file__), 'static'),
                              template_path=os.path.join(os.path.dirname(__file__), 'template'),
                              autoescape=None,  # 全局关闭模板转义功能
                              )
        if self.cerfile and keyfile:
            wsServer = httpserver.HTTPServer(app, ssl_options=ssl_options)
        else:
            wsServer = httpserver.HTTPServer(app)
        wsServer.listen(options.websocket_service_port)

    @gen.coroutine
    def checkClientHeart(self):
        now_time = time.time()
        for clientObject in self.clientSet:
            if now_time - clientObject.lastHeartbeat >= 30:
                clientObject.close()
                self.clientSet.discard(clientObject)
                del clientObject
                self.ioloop.add_timeout(self.ioloop.time() + 1, self.checkClientHeart)
                break
        self.ioloop.add_timeout(self.ioloop.time() + 10, self.checkClientHeart)