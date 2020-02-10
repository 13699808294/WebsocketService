import json

from tornado import gen

from apps.websocket.websocketService import WebsocketService
from setting.setting import QOS_LOCAL, CER_FILE, KEY_FILE
from utils.bMosquittoClient import BMosquittoClient
from utils.baseAsync import BaseAsync

from utils.logClient import logClient
from utils.my_json import json_dumps


class MosquittoClient(BMosquittoClient,BaseAsync):
    def __init__(self,*args,**kwargs):
        super().__init__(*args,**kwargs)
        self.heartTopic = '/aaiot/websocketService/send/controlbus/system/heartbeat'
        self.websocketService = WebsocketService(mosquittoClient=self,cerfile=CER_FILE,keyfile=KEY_FILE)
        self.clientSet = self.websocketService.clientSet
        self.typeToFunction = {
            'channel_feedback':self.channelFeedback,
            'status_feedback': self.statusFeedback,
            'online_info': self.deviceOnline,
            'device_switch': self.deviceSwitch,
            'user_schedule_event': self.userSchedule,
            'schedule_time_change': self.scheduleTimeChange,
            'schedule_info': self.scheduleInfo,
            'separate_change': self.sparateChange,
            'city_weather': self.cityWeather,
            'info': self.askInfo,
            'control': self.askInfo,
            'query': self.askInfo,
            'meeting_room_status': self.askInfo,
            'meeting_room_device': self.askInfo,
            'device_channel': self.askInfo,
            'meeting_room_schedule':self.askInfo,

        }

    @gen.coroutine
    def handleOnMessage(self,mReceiveMessageObject):
        data = mReceiveMessageObject.data
        topic_list = mReceiveMessageObject.topicList

        if topic_list[5] == 'websocket':
            yield self.websocketSync(data)
        elif topic_list[5] == 'device':
            yield self.deviceAuthorizationHandle(mReceiveMessageObject)

    @gen.coroutine
    def deviceAuthorizationHandle(self, mReceiveMessageObject):
        '''
        data = {
            'topic': message.topic,
            'msg': message.payload.decode(),
            'topic_list': topic_list
        }
        '''
        channel = mReceiveMessageObject.channel
        data = mReceiveMessageObject.data
        try:
            authorization_status = int(data)
        except:
            return
        device_guid = channel
        #
        for websocketClientObject in self.clientSet:
            for connection_token, user_info in websocketClientObject.login_info.items():
                authorization_device_guid = user_info.get('device_guid')
                if authorization_device_guid == device_guid:
                    user_info['authorization_status'] = authorization_status

    @gen.coroutine
    def channelFeedback(self,clientObject,connection_token, user_info,msg):
        authorization_status = user_info.get('authorization_status')
        if not authorization_status:
            return
        use_room_list = user_info.get('use_room_list')
        if not isinstance(use_room_list, list):
            return
        msg_meeting_room_guid = msg.get('meeting_room_guid')
        if msg_meeting_room_guid in use_room_list:
            msg['connection_token'] = connection_token
            yield clientObject.sendToClient(json_dumps(msg))

    @gen.coroutine
    def statusFeedback(self,clientObject,connection_token, user_info,msg):
        authorization_status = user_info.get('authorization_status')
        if not authorization_status:
            return
        use_room_list = user_info.get('use_room_list')
        if not isinstance(use_room_list, list):
            return
        msg_meeting_room_guid = msg.get('meeting_room_guid')
        if msg_meeting_room_guid in use_room_list:
            msg['connection_token'] = connection_token
            yield clientObject.sendToClient(json_dumps(msg))

    @gen.coroutine
    def deviceOnline(self,clientObject,connection_token, user_info,msg):
        user_type = user_info['user_type']
        if user_type == 'back-end':
            use_room_list = user_info.get('use_room_list')

            if not isinstance(use_room_list, list):
                return
            msg_meeting_room_guid = msg.get('meeting_room_guid')
            if msg_meeting_room_guid in use_room_list:
                # 添加一个connection_token以便代理推送
                msg['connection_token'] = connection_token
                yield clientObject.sendToClient(json_dumps(msg))

    @gen.coroutine
    def deviceSwitch(self,clientObject,connection_token, user_info,msg):
        user_type = user_info['user_type']
        if user_type == 'back-end':
            use_room_list = user_info.get('use_room_list')

            if not isinstance(use_room_list, list):
                return
            msg_meeting_room_guid = msg.get('meeting_room_guid')
            if msg_meeting_room_guid in use_room_list:
                # 添加一个connection_token以便代理推送
                msg['connection_token'] = connection_token
                yield clientObject.sendToClient(json_dumps(msg))

    @gen.coroutine
    def userSchedule(self,clientObject,connection_token, user_info,msg):
        user_id = user_info.get('user_id')
        msg_user_id = msg.get('user_id')
        if user_id == msg_user_id:
            msg['connection_token'] = connection_token
            yield clientObject.sendToClient(json_dumps(msg))

    @gen.coroutine
    def scheduleTimeChange(self,clientObject,connection_token, user_info,msg):
        user_type = user_info.get('user_type')
        if user_type == 'back-end':
            msg['connection_token'] = connection_token
            yield clientObject.sendToClient(json_dumps(msg))

    @gen.coroutine
    def scheduleInfo(self,clientObject,connection_token, user_info,msg):
        user_type = user_info.get('user_type')
        if user_type == 'back-end':
            msg['connection_token'] = connection_token
            yield clientObject.sendToClient(json_dumps(msg))

    @gen.coroutine
    def sparateChange(self,clientObject,connection_token, user_info,msg):
        mode = msg.get('mode')
        meeting_room_info = msg.get('meeting_room_info')
        result = False
        use_room_list = user_info.get('use_room_list')
        if mode == 0:  # 合房模式
            separate_meeting_room_guid_list = meeting_room_info.get('separate_meeting_room_guid_list')
            for room_guid in use_room_list:
                if room_guid in separate_meeting_room_guid_list:
                    result = True
                    break
        elif mode == 1:  # 分房模式
            real_meeting_room_guid = meeting_room_info.get('real_meeting_room_guid')
            if real_meeting_room_guid in use_room_list:
                result = True
        else:
            return
        if result:
            data = {
                'type': 'separate_out',
                'mode': mode,
                'connection_token': connection_token
            }
            yield clientObject.sendToClient(json_dumps(data))

    @gen.coroutine
    def cityWeather(self,clientObject,connection_token, user_info,msg):
        city_guid = user_info.get('city_guid')

        if city_guid == msg.get('guid'):
            msg_connection_token = msg.get('connection_token')
            if msg_connection_token:
                if msg_connection_token == connection_token:
                    msg['connection_token'] = connection_token
                    yield clientObject.sendToClient(json_dumps(msg))
            else:
                msg['connection_token'] = connection_token
                yield clientObject.sendToClient(json_dumps(msg))

    @gen.coroutine
    def askInfo(self,clientObject,connection_token, user_info,msg):
        msg_connection_token = msg.get('connection_token')
        if msg_connection_token == connection_token:
            yield clientObject.sendToClient(json_dumps(msg))

    @gen.coroutine
    def websocketSync(self, data):
        try:
            data = json.loads(data)
        except:
            return
        msg_type = data.get('type')
        if msg_type == None:
            return
        yield logClient.tornadoDebugLog('同步信息：类型为{}'.format(data.get('type')))

        for clientObject in self.clientSet:
            for connection_token, user_info in clientObject.login_info.items():
                func = self.typeToFunction.get(msg_type)
                if func == None:
                        return
                yield func(clientObject,connection_token, user_info,data)

    @gen.coroutine
    def handle_on_connect(self):
        topic = '/aaiot/mqttService/receive/controlbus/system/heartbeat'
        self.connectObject.subscribe(topic, qos=QOS_LOCAL)
        yield logClient.tornadoDebugLog('订阅主题:({}),qos=({})'.format(topic, QOS_LOCAL))

        topic = '/aaiot/+/send/controlbus/event/websocket/#'
        self.connectObject.subscribe(topic, qos=QOS_LOCAL)
        yield logClient.tornadoDebugLog('订阅主题:({}),qos=({})'.format(topic, QOS_LOCAL))

        topic = '/aaiot/+/send/controlbus/event/device/update_authorization/#'
        self.connectObject.subscribe(topic, qos=QOS_LOCAL)
        yield logClient.tornadoDebugLog('订阅主题:({}),qos=({})'.format(topic, QOS_LOCAL))

