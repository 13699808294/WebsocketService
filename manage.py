import asyncio

import uvloop
from tornado.platform.asyncio import BaseAsyncIOLoop

from apps.mosquittoClient.mosquittoClient import MosquittoClient
from apps.httpService import HttpService
from setting.setting import BUILD_SERVER_HOST

if __name__ == '__main__':
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())  # 修改循环策略为uvloop
    aioloop = asyncio.get_event_loop()  # 获取aioloop循环事件
    ioloop = BaseAsyncIOLoop(aioloop)  # 使用aioloop创建ioloop

    mosquittoClient = MosquittoClient(host=BUILD_SERVER_HOST)
    HttpService()
    ioloop.current().start()