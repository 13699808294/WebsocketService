import datetime
import json
import time
from urllib.parse import urlparse

from tornado.websocket import WebSocketHandler
from tornado import gen

from setting.setting import WEBSOCKETALLOWTYPE, QOS_LOCAL
from utils.baseAsync import BaseAsync

from utils.MysqlClient import mysqlClient
from utils.logClient import logClient
from utils.my_json import json_dumps


class WebsocketClient(WebSocketHandler):
    def check_origin(self, origin: str) -> bool:
        # parsed_origin = urlparse(origin)
        # origin = parsed_origin.netloc
        # origin = origin.lower()
        # host = self.request.headers.get("Host")
        # Check to see that origin matches host directly, including ports
        # origin = origin.split(':')[0]
        # host = host.split(':')[0]
        return True

    def initialize(self, server):
        self.server = server  # WebsocketServer对象
        self.mosquittoClient = server.mosquittoClient
        self.ioloop = server.ioloop  #
        self.clientSet = server.clientSet
        self.lastHeartbeat = time.time()
        self.login_info = {}

        self.typeToFunction = {
            'login':self.loginHandle,
            'heartbeat':self.headerbeatHandle,
            'logout':self.logoutHandle,
            'info':self.infoHandler,
            'control':self.controlHandle,
            'query':self.queryHandle,
            'meeting_room_status':self.meetingRoomStatusHandle,
            'meeting_room_device':self.meetingRoomDeviceHandle,
            'device_channel':self.deviceChannelHandle,
            'separate_info':self.separateInfoHandle,
            'weather_info':self.weatherInfoHandle,
            'meeting_room_schedule':self.meetingRoomScheduleHandle,
        }

    #todo: 检测登录状态
    @gen.coroutine
    def checkLoginState(self,msg_type,connection_token,):
        # 判断是否登录
        try:
            login_info = self.login_info[connection_token]
        except:
            yield self.baseErrorReturn(msg_type,'The user has no login information',connection_token)
            return False
        is_login = self.login_info[connection_token].get('login_status')
        if not is_login:
            yield self.baseErrorReturn(msg_type, 'The user login status is not logged in', connection_token)
            return False
        is_authorization = self.login_info[connection_token].get('authorization_status')
        if not is_authorization:
            yield self.baseErrorReturn(msg_type, 'Equipment not authorized', connection_token)
            return False
        # user_type = login_info.get('user_type')
        # user_name = login_info.get('user_name')
        # company_db = login_info.get('company_db')
        # if user_type == None or user_name == None or company_db == None:
        #     yield self.baseErrorReturn(msg_type, 'The user login status is abnormal', connection_token)
        #     return False
        return login_info

    #todo: 登录类型处理
    @gen.coroutine
    def loginHandle(self,msg_type,msg):
        connection_token = msg.get('connection_token')
        user_type = msg.get('user_type')
        user_id = msg.get('user_id')
        user_token = msg.get('token')
        host = msg.get('host')
        device_sn = msg.get('device_sn')
        company_db = msg.get('company_db')

        #0: 公司代码校验
        if company_db == 'aaiot':
            company_db = 'aura'
        elif not company_db:
            yield self.baseErrorReturn(msg_type, 'Company code error', connection_token)
            return
        #1: 设备授权校验
        if device_sn == None or device_sn == '':
            authorization_status = 1
            device_guid = ''
        else:
            # 判断授权
            check_info = yield self.deviceAuthorizationInfo(device_sn, company_db)
            authorization_status = check_info.get('authorization_status')
            device_guid = check_info.get('guid')
            if authorization_status == 0:
                yield self.baseErrorReturn(msg_type,'Equipment not authorized',connection_token)
                return
        #2: 用户类型校验
        if user_type not in WEBSOCKETALLOWTYPE:
            yield self.baseErrorReturn(msg_type, 'Invalid user type', connection_token)
            return
        #3: 获取用户名称
        user_name = yield self.userName(user_id, company_db)
        if user_name == '':
            yield self.baseErrorReturn(msg_type, 'The user does not exist', connection_token)
            return

        #4: 多次登录判断
        if connection_token in self.login_info.keys():
            yield self.baseErrorReturn(msg_type, 'Client Multiple logins', connection_token)
            return

        # 5: 后台登录token校验
        result = yield self.userLoginTokenCheck(user_id, user_token, company_db)
        if result == False:
            yield self.baseErrorReturn(msg_type, 'User token validation failed', connection_token)
            return
        #6: 用户所有会议室权限获取
        if user_type == 'ipad':
            room_list = yield self.userRightsMeetingRoom(user_id, company_db)
        elif user_type == 'back-end':
            room_list = yield self.allMeetingRoom(company_db)
        else:
            yield self.baseErrorReturn(msg_type, 'Unable to get info', connection_token)
            return
        # 保存登录信息
        info = {
            'user_id': user_id,
            'user_name': user_name,
            'user_token': user_token,
            'company_db': company_db,
            'user_type': user_type,
            'authorization_status': authorization_status,
            'device_sn': device_sn,
            'device_guid': device_guid,
            'login_status': True,
            'room_list': [x['meeting_room_guid_id'] for x in room_list],    #权限会议室
            'host': host,
            'use_room_list':[]
        }
        self.login_info[connection_token] = info
        if user_type == 'back-end':
            self.login_info[connection_token]['use_room_list'] = [x['meeting_room_guid_id'] for x in room_list]
        # 返回登录成功状态
        content = {
            'ret': 0,
            'type': 'login',
            'room_list': room_list,
            'connection_token': connection_token
        }
        self.sendToClient(json_dumps(content))
        return

    #todo: 心跳类型处理
    @gen.coroutine
    def headerbeatHandle(self,msg_type,msg):
        self.lastHeartbeat = time.time()
        return

    #todo: 登出类型处理
    @gen.coroutine
    def logoutHandle(self, msg_type, msg):
        connection_token = msg.get('connection_token')
        if connection_token in self.login_info.keys():
            del self.login_info[connection_token]
            yield logClient.tornadoInfoLog('用户{},推出登录'.format(connection_token))
        return

    #todo: 获取info类型处理
    @gen.coroutine
    def infoHandler(self, msg_type, msg):
        connection_token = msg.get('connection_token')
        login_info = yield self.checkLoginState(msg_type,connection_token)
        if login_info == False:
            return
        user_type = login_info.get('user_type')
        user_name = login_info.get('user_name')
        yield logClient.tornadoInfoLog('用户 {} 获取权限内会议室所有控制通道'.format(user_name))
        control_info = msg.get('control_info')
        if control_info == '' or control_info == None:
            control_info = 0
        if user_type == 'ipad':
            # use_room_list类型判断
            use_room_list = msg.get('use_room_list')
            if not isinstance(use_room_list, list):
                yield self.baseErrorReturn(msg_type,'use_room_list Type error',connection_token)
                return
            # use_room_list权限判断
            for room_guid in use_room_list:
                if room_guid not in self.login_info[connection_token]['room_list']:
                    yield self.baseErrorReturn(msg_type, 'The whole school without meeting rooms {}'.format(room_guid), connection_token)
                    return
            use_room_list = list(set(use_room_list))
        elif user_type == 'back-end':
            use_room_list = login_info.get('room_list')
        else:
            yield self.baseErrorReturn(msg_type, 'Unable to get info', connection_token)
            return
        self.login_info[connection_token]['use_room_list'] = use_room_list
        topic = '/aaiot/0/receive/controlbus/event/websocket/0/info'
        msg = {
            'ret': 0,
            'type': msg_type,
            'control_info': control_info,
            'use_room_list': use_room_list,
            'connection_token': connection_token,
        }
        self.mosquittoClient.myPublish(topic, json_dumps(msg), QOS_LOCAL)

    @gen.coroutine
    def controlHandle(self, msg_type, msg):
        connection_token = msg.get('connection_token')
        login_info = yield self.checkLoginState(msg_type, connection_token)
        if login_info == False:
            return
        # 矩阵控制前端需要的数据
        # msg = {
        #     'guid':'',
        #     'meeting_room_guid':'',
        #     'control_flag':4,
        #     'event_type':'click',
        #     'input_port':'输入段guid',
        #     'status':'当前有为on,没有连接为off',
        # }
        user_type = login_info.get('user_type')
        user_id = login_info.get('user_id', '')
        channel_guid = msg.get('guid')
        meeting_room_guid = msg.get('meeting_room_guid')
        control_type = msg.get('control_flag', 1)
        event_type = msg.get('event_type')
        status = msg.get('status')
        # 添加兼容矩阵控制
        input_port = msg.get('input_port')

        # 1  正常模式
        # 0  不发送MQTT指令
        # 2  不回control信息
        # 3  不回control信息,不发送MQTT指令
        # 判断控制方式是否正确
        try:
            control_type = int(control_type)
        except:
            if control_type not in [0,1,2,3]:
                yield self.baseErrorReturn(msg_type,'control_type parameter error',connection_token)
                return
        if user_type == 'applet' or user_type == 'front-end':
            yield self.baseErrorReturn(msg_type, 'No control permission',connection_token)
            return
        elif user_type == 'back-end' or user_type == 'ipad':
            topic = '/aaiot/0/receive/controlbus/event/websocket/0/{}'.format(msg_type)
            msg = {
                'ret': 0,
                'type': msg_type,
                'meeting_room_guid': meeting_room_guid,
                'channel_guid': channel_guid,
                'user_id': user_id,
                'event_type': event_type,
                'status': status,
                'control_type': control_type,
                'connection_token': connection_token,
            }
            # 添加兼容矩阵控制
            if input_port != None:
                msg['input_port'] = input_port
            self.mosquittoClient.myPublish(topic, json_dumps(msg), QOS_LOCAL)
            return

    @gen.coroutine
    def queryHandle(self, msg_type, msg):
        yield self.controlHandle(msg_type,msg)

    @gen.coroutine
    def meetingRoomStatusHandle(self, msg_type, msg):
        connection_token = msg.get('connection_token')
        login_info = yield self.checkLoginState(msg_type, connection_token)
        if login_info == False:
            return
        meeting_room_guid = msg.get('meeting_room_guid')
        company_db = login_info.get('company_db')
        topic = '/aaiot/0/receive/controlbus/event/websocket/0/meeting_room_status'
        msg = {
            'ret': 0,
            'type': msg_type,
            'company_db': company_db,
            'meeting_room_guid':meeting_room_guid,
            'connection_token': connection_token,
        }
        self.mosquittoClient.myPublish(topic, json_dumps(msg), QOS_LOCAL)
        return

    @gen.coroutine
    def meetingRoomScheduleHandle(self,msg_type,msg):
        connection_token = msg.get('connection_token')
        login_info = yield self.checkLoginState(msg_type, connection_token)
        if login_info == False:
            return
        meeting_room_guid = msg.get('meeting_room_guid')
        topic = '/aaiot/0/receive/controlbus/event/websocket/0/meeting_room_schedule'
        msg = {
            'ret': 0,
            'type': msg_type,
            'meeting_room_guid': meeting_room_guid,
            'connection_token': connection_token,
        }
        self.mosquittoClient.myPublish(topic, json_dumps(msg), QOS_LOCAL)
        return

    @gen.coroutine
    def meetingRoomDeviceHandle(self, msg_type, msg):
        connection_token = msg.get('connection_token')
        login_info = yield self.checkLoginState(msg_type, connection_token)
        if login_info == False:
            return
        meeting_room_guid = msg.get('meeting_room_guid')
        company_db = login_info.get('company_db', '')
        topic = '/aaiot/0/receive/controlbus/event/websocket/0/meeting_room_device'
        msg = {
            'ret': 0,
            'type': msg_type,
            'company_db': company_db,
            'connection_token': connection_token,
        }
        if meeting_room_guid != None:
            msg['meeting_room_guid'] = meeting_room_guid
        self.mosquittoClient.myPublish(topic, json_dumps(msg), QOS_LOCAL)
        return

    @gen.coroutine
    def deviceChannelHandle(self, msg_type, msg):
        connection_token = msg.get('connection_token')
        login_info = yield self.checkLoginState(msg_type, connection_token)
        if login_info == False:
            return
        meeting_room_guid = msg.get('meeting_room_guid')
        device_guid = msg.get('device_guid')
        topic = '/aaiot/0/receive/controlbus/event/websocket/0/device_channel'
        msg = {
            'ret': 0,
            'type': msg_type,
            'device_guid': device_guid,
            'meeting_room_guid': meeting_room_guid,
            'connection_token': connection_token,
        }
        self.mosquittoClient.myPublish(topic, json_dumps(msg), QOS_LOCAL)
        return

    @gen.coroutine
    def separateInfoHandle(self, msg_type, msg):
        connection_token = msg.get('connection_token')
        login_info = yield self.checkLoginState(msg_type, connection_token)
        if login_info == False:
            return
        meeting_room_guid = msg.get('meeting_room_guid')
        company_db = login_info.get('company_db')
        separate_info_list = yield self.getSeparateInfo(meeting_room_guid, company_db)
        if separate_info_list == False:
            yield self.baseErrorReturn(msg_type, 'get Separate Info Error', connection_token)
            return
        content = {
            'type': msg_type,
            'ret': 0,
            'separate_info_list': separate_info_list,
            'connection_token': connection_token
        }
        return self.sendToClient(json_dumps(content))

    @gen.coroutine
    def weatherInfoHandle(self, msg_type, msg):
        connection_token = msg.get('connection_token')
        login_info = yield self.checkLoginState(msg_type, connection_token)
        if login_info == False:
            return
        city_guid = msg.get('city_guid')
        login_info['city_guid'] = city_guid
        content = {
            'type': msg_type,
            'ret': 0,
            'city_guid': city_guid,
            'connection_token': connection_token
        }
        topic = '/aaiot/{}/receive/controlbus/event/websocket/city_weather/0'.format(city_guid)
        self.mosquittoClient.myPublish(topic, json_dumps(content), QOS_LOCAL)
        return

    @gen.coroutine
    def on_message(self, message):
        try:
            msg = json.loads(message)
        except:
            try:
                msg = eval(message)
            except:
                return
        #获取操作类型
        msg_type = msg.get('type')
        func = self.typeToFunction.get(msg_type)
        if func == None:
            return
        yield func(msg_type, msg)

    @gen.coroutine
    def deviceAuthorizationInfo(self,device_sn,company_db):
        data = {
            'database': company_db,
            'fields': ['authorization_status'],
            'eq': {
                'is_delete': False,
                'sn': device_sn,
            },
        }
        msg = yield mysqlClient.tornadoSelectOnly('d_device', data)
        if msg['ret'] != '0':
            return False
        else:
            if msg['lenght'] == 1:
                return msg['msg'][0]
            else:
                return {'guid':'','authorization_status':0}

    @gen.coroutine
    def userName(self,user_id,company_db):
        # 获取所有会议室
        data = {
            'database': company_db,
            'fields': ['username'],
            'eq': {
                'is_delete': False,
                'guid':user_id
            },
        }
        msg = yield mysqlClient.tornadoSelectOnly('user', data)
        if msg['ret'] == '0' and msg['lenght'] == 1:
            return  msg['msg'][0]['username']
        else:
            return ''

    @gen.coroutine
    def userLoginTokenCheck(self, user_id, token, company_db):
        # 数据库查询token,tken存在,且创建时间在十分钟内
        data = {
            'database': company_db,
            'fields': ['user_id'],
            'eq': {
                'is_delete': False,
                'token': token,
                'user_id': user_id,
            },
            'gte': {
                'update_time': datetime.datetime.now() - datetime.timedelta(days=1),  # 最迟是五分钟前的数据
            },
            'lte': {
                'update_time': datetime.datetime.now() + datetime.timedelta(seconds=30),  # 小于等于当前时间
            },
        }
        msg = yield mysqlClient.tornadoSelectOnly('user_token', data)
        if msg['ret'] != '0' and msg['lenght'] == 1:
            return False
        else:
            return True

    @gen.coroutine
    def userRightsMeetingRoom(self, user_id, company_db):
        meeting_room_guid_list = []
        # 获取用户所在群组
        data = {
            'database': company_db,
            'fields': ['group_guid_id'],
            'eq': {
                'is_delete': False,
                'user_guid_id': user_id
            },
        }
        msg = yield mysqlClient.tornadoSelectAll('d_user_group', data)
        groups = []
        if msg['ret'] == '0':
            groups = msg['msg']
        else:
            return meeting_room_guid_list
        for group in groups:
            # 获取群组下所有会议室
            data = {
                'database': company_db,
                'fields': [
                    'd_meeting_room_group.meeting_room_guid_id',
                    'd_meeting_room.room_name'],
                'eq': {
                    'd_meeting_room_group.is_delete': False,
                    'd_meeting_room.is_delete': False,
                    'd_meeting_room.guid': {'key': 'd_meeting_room_group.meeting_room_guid_id'},
                    'd_meeting_room_group.group_guid_id': group['group_guid_id']
                },
            }
            msg = yield mysqlClient.tornadoSelectAll('d_meeting_room_group,d_meeting_room', data)
            if msg['ret'] == '0':
                meeting_room_guid_list += msg['msg']
        return meeting_room_guid_list

    @gen.coroutine
    def allMeetingRoom(self, company_db):
        # 获取所有会议室
        data = {
            'database': company_db,
            'fields': ['guid as meeting_room_guid_id', 'room_name'],
            'eq': {
                'is_delete': False,
                'virtual_guid': '',
            },
        }
        msg = yield mysqlClient.tornadoSelectAll('d_meeting_room', data)
        if msg['ret'] == '0':
            return msg['msg']
        else:
            return []

    # todo：新功能
    @gen.coroutine
    def getSeparateInfo(self, meeting_room_guid, company_db):
        # 判断传入会议室guid是实体会议室还是分房会议室
        data = {
            'database': company_db,
            'fields': ['virtual_guid'],
            'eq': {
                'is_delete': False,
                'guid': meeting_room_guid,
            }
        }
        msg = yield mysqlClient.tornadoSelectOnly('d_meeting_room', data)
        if msg['ret'] == '0' and msg['lenght'] == 1:
            virtual_guid = msg['msg'][0]['virtual_guid']
        else:
            return False
        if virtual_guid == '':
            # 实体会议室
            real_meeting_room_guid_list = [meeting_room_guid]
        elif virtual_guid == 'separate':
            # 获取分房会议室所在的实体会议室
            data = {
                'database': company_db,
                'fields': ['real_meeting_room_guid_id'],
                'eq': {
                    'is_delete': False,
                    'separate_meeting_room_guid_id': meeting_room_guid
                },
            }
            msg = yield mysqlClient.tornadoSelectAll('d_separate_meeting_room', data)
            if msg['ret'] == '0':
                real_meeting_room_guid_list = [x['real_meeting_room_guid_id'] for x in msg['msg']]
            else:
                return False
        else:
            return False
        separate_info_list = []
        for real_meeting_room_guid in real_meeting_room_guid_list:
            data = {
                'database': company_db,
                'fields': ['guid as real_meeting_room_guid_id', 'room_name'],
                'eq': {
                    'guid': real_meeting_room_guid
                }
            }
            msg = yield mysqlClient.tornadoSelectAll('d_meeting_room', data)
            if msg['ret'] == '0':
                separate_info_list += msg['msg']
            else:
                return False
            # 分房会议室
            data = {
                'database': company_db,
                'fields': ['d_separate_meeting_room.separate_meeting_room_guid_id',
                           'd_meeting_room.room_name'],
                'eq': {
                    'd_separate_meeting_room.is_delete': False,
                    'd_meeting_room.is_delete': False,
                    'd_separate_meeting_room.separate_meeting_room_guid_id': {'key': 'd_meeting_room.guid'},
                    'real_meeting_room_guid_id': real_meeting_room_guid
                },
            }
            # 获取所有会议室
            msg = yield mysqlClient.tornadoSelectAll('d_separate_meeting_room,d_meeting_room', data)
            if msg['ret'] == '0':
                separate_info_list += msg['msg']
            else:
                return False
        return separate_info_list

    @gen.coroutine
    def open(self, *args: str, **kwargs: str):
        self.meeting_room_guid = None
        if self not in self.clientSet:
            self.clientSet.add(self)
        yield logClient.tornadoDebugLog('websocket连接接入,当前登录用户数量{}'.format(len(self.clientSet)))

    @gen.coroutine
    def sendToClient(self, msg):
        try:
            self.write_message(msg)
        except:
            self.close()

    @gen.coroutine
    def on_close(self) -> None:
        # 从server的websocket对象集合中移除自己
        self.clientSet.discard(self)
        yield logClient.tornadoDebugLog('websocket断开连接,当前登录用户数量{}'.format(len(self.clientSet)))
        del self

    @gen.coroutine
    def baseErrorReturn(self,msg_type,return_msg,connection_token):
        msg = {
            'ret':1,
            'type':msg_type,
            'errmsg':return_msg,
            'connection_token':connection_token
        }
        yield self.sendToClient(json_dumps(msg))