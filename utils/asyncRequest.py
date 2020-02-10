import json
from json import JSONDecodeError


from tornado import gen, httpclient
from tornado.httputil import url_concat

from utils.my_json import json_dumps


@gen.coroutine
def asyncTornadoRequest(
        url:str,
        params:dict=None,
        body:dict=None,
        method:str='GET',
        headers:dict=None,
        allow_nonstandard_methods:bool=False
        ):
    if body:
        body = json_dumps(body)
    if params:
        url = url_concat(url, params)
    request = httpclient.HTTPRequest(
        url=url,
        headers=headers,
        method=method,
        body=body,
        allow_nonstandard_methods=allow_nonstandard_methods,
    )
    http = httpclient.AsyncHTTPClient()
    try:
        resp = yield http.fetch(request)
        result = json.loads(resp.body.decode())
    except Exception as e:
        result = { "status": 1, "errmsg": e }
    return result
