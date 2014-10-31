import httplib, urllib
import simplejson as json
import time
from collections import deque
import pdb


HOST = 'instagram.com'
API_HOST = 'api.instagram.com'

ENTITY_TYPES = ['feed', 'image', 'user']

SEED_USERS = ['1418652011']
class InstagramCrawler():

    def __init__(self):
        self.client_id = '9936fc4ff90f4344a2384c35766525c6'
        self.client_secret = '5980acbbca814e4f94c3ff19beef7673'
        self.redirect_uri = 'https://www.ntu.edu.sg/'

        self.conn = {}

        self.queues = {}


    def request(self, method='GET', host=HOST, path='', headers={}, data=None):
        if host not in self.conn:
            self.conn[host] = httplib.HTTPSConnection(host)

        conn = self.conn[host]

        if method == 'POST' and data:
            conn.request(method, path, body=data, headers=headers)
        else:
            conn.request(method, path, headers=headers)

        r = conn.getresponse()

        res_headers = {k:v for k,v in r.getheaders()}
        content = r.read()

        time.sleep(1)

        
        return r.status, res_headers, content


    def get_token_access(self):
        path = '/oauth/authorize/?client_id=%s&redirect_uri=%s&response_type=code' % (self.client_id, urllib.quote(self.redirect_uri))
        headers = {}

        status, r_headers, content = self.request('GET', path=path, headers=headers)

        url = r_headers['location']
        path = url[url.find('/', 8):]

        csrftoken = r_headers['set-cookie'].split('csrftoken=')[1].split(';')[0]
        headers['Cookie'] = 'csrftoken=%s' % csrftoken

        status, r_headers, content = self.request('GET', path=path, headers=headers)

        mid = r_headers['set-cookie'].split("mid=")[1].split(";")[0]
        headers['Cookie'] += '; mid=%s' % mid

        headers['Referer'] = url 
        data = 'csrfmiddlewaretoken=%s&username=pntuananh&password=swordfish' % csrftoken

        status, r_headers, content = self.request('POST', path=path, headers=headers, data=data)
        
        url = r_headers['location']
        path = url[url.find('/', 8):]
        headers['Referer'] = url 

        cookie = r_headers['set-cookie']
        sessionid = cookie.split('sessionid=')[1].split(';')[0]
        headers['Cookie'] += '; sessionid=%s' % sessionid

        status, r_headers, content = self.request('GET', path=path, headers=headers)

        path = '/oauth/authorize/?client_id=%s&redirect_uri=%s&response_type=code' % (self.client_id, urllib.quote(self.redirect_uri))
        data = 'csrfmiddlewaretoken=%s&allow=Authorize' % csrftoken

        status, r_headers, content = self.request('POST', path=path, headers=headers, data=data)

        url = r_headers['location']
        code = url.split('code=')[1]

        client_params = {
            "client_id"     : self.client_id,
            "client_secret" : self.client_secret,
            "redirect_uri"  : self.redirect_uri,
            "grant_type"    : "authorization_code",
            'code'          : code,
        }

        path = '/oauth/access_token'

        status, r_headers, content = self.request('POST', host=API_HOST, path=path, data=urllib.urlencode(client_params))

        js =  json.loads(content)

        return js['access_token']

