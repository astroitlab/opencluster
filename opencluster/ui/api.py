import web

apiUrls = [
    "/api/hello", "Hello",
    "/api/health", "Health"
]
def operator_status(func):
    '''''get operatoration status
    '''
    def gen_status(*args, **kwargs):
        error, result = None, None
        try:
            result = func(*args, **kwargs)
        except Exception as e:
            error = str(e)
        return {'result': result, 'error':  error}
    return gen_status


web.config.debug = True

class Hello(object):
    def GET(self):
        return "hello "
    def PUT(self):
        return "hello "

class Health(object):
    def GET(self):
        return "ok"
    def PUT(self):
        return "ok"
