import httplib, urllib, urllib2
import simplejson as json
import time, datetime
import os, thread
from collections import deque
import pdb


HOST         = 'instagram.com'
API_HOST     = 'api.instagram.com'
MEDIA_SEARCH = '/v1/media/search'
USER_INFO    = '/v1/users'

DATA_DIR = 'data/'
IMAGE_DIR = DATA_DIR + 'images/'

ENTITY_TYPES = ['image', 'user', 'follow']

MAX_N_ITEM = 10000
TO_FLUSH = 100

regions = {
        'NYC': {
            'lat1' : 40.54406959,
            'lon1' : -74.04716492,
            'lat2' : 40.89586771,
            'lon2' : -73.73130798,
            },
        }

delta_lat = 0.046
delta_lon = 0.059

start_time = '31/10/2013'
end_time = '31/10/2014'

oneday = 86400

class InstagramCrawler():

    def __init__(self, reload=False):
        self.client_id = '9936fc4ff90f4344a2384c35766525c6'
        self.client_secret = '5980acbbca814e4f94c3ff19beef7673'
        self.redirect_uri = 'https://www.ntu.edu.sg/'

        self.conn = {}
        self.access_token = self.get_token_access()

        self.seen = {}
        self.nu = {}
        self.files = {}

        if not reload:
            for typ in ENTITY_TYPES:
                self.seen[typ] = set()
                self.nu[typ] = 0
                self.files[typ] = self.open_file(typ, 0)

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
        print 'Requesting token access...',

        path = '/oauth/authorize/?client_id=%s&redirect_uri=%s&response_type=code' % (self.client_id, urllib.quote(self.redirect_uri))
        self.headers = {}

        status, r_headers, content = self.request('GET', path=path, headers=self.headers)

        url = r_headers['location']
        path = url[url.find('/', 8):]

        csrftoken = r_headers['set-cookie'].split('csrftoken=')[1].split(';')[0]
        self.headers['Cookie'] = 'csrftoken=%s' % csrftoken

        status, r_headers, content = self.request('GET', path=path, headers=self.headers)

        mid = r_headers['set-cookie'].split("mid=")[1].split(";")[0]
        self.headers['Cookie'] += '; mid=%s' % mid

        self.headers['Referer'] = url 
        data = 'csrfmiddlewaretoken=%s&username=pntuananh&password=swordfish' % csrftoken

        status, r_headers, content = self.request('POST', path=path, headers=self.headers, data=data)
        
        url = r_headers['location']
        path = url[url.find('/', 8):]
        self.headers['Referer'] = url 

        cookie = r_headers['set-cookie']
        sessionid = cookie.split('sessionid=')[1].split(';')[0]
        self.headers['Cookie'] += '; sessionid=%s' % sessionid

        status, r_headers, content = self.request('GET', path=path, headers=self.headers)

        path = '/oauth/authorize/?client_id=%s&redirect_uri=%s&response_type=code' % (self.client_id, urllib.quote(self.redirect_uri))
        data = 'csrfmiddlewaretoken=%s&allow=Authorize' % csrftoken

        status, r_headers, content = self.request('POST', path=path, headers=self.headers, data=data)

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

        print 'Done'

        return js['access_token']


    def open_file(self, prefix, index, mode='w'):
        f = open(DATA_DIR + '%s%04d.txt' % (prefix,index), mode)
        return f


    def download(self, url):
        filename = url.split('/')[-1]

        retry = 3
        image = None
        while retry:
            try:
                image = urllib2.urlopen(url).read()
                break
            except:
                retry -= 1

        if image:
            directory = IMAGE_DIR + filename[:5]
            if not os.path.exists(directory):
                os.makedirs(directory)
            
            filename = '%s/%s' % (directory,filename)
            open(filename, 'wb').write(image)


    def write(self, typ, item):
        self.files[typ].write('%s' % item)
        if item[-1] != '\n':
            self.files[typ].write('\n')
        
        nu = self.nu[typ] = self.nu[typ] + 1  
        if nu % MAX_N_ITEM == 0:
            self.files[typ].close()
            self.files[typ] = self.open_file(typ, nu/MAX_N_ITEM)
        elif nu % TO_FLUSH == 0:
            self.files[typ].flush()
            os.fsync(self.files[typ].fileno())


    def get_images_by_region(self,reg):
        thread.start_new_thread(self.display, ())

        start_timestamp = int(time.mktime(datetime.datetime.strptime(start_time, "%d/%m/%Y").timetuple()))
        end_timestamp = int(time.mktime(datetime.datetime.strptime(end_time, "%d/%m/%Y").timetuple()))

        geo = regions[reg]
        lat1 = geo['lat1']
        lon1 = geo['lon1']
        lat2 = geo['lat2']
        lon2 = geo['lon2']

        for e_ts in xrange(end_timestamp, start_timestamp,-7*oneday):
            s_ts = e_ts - 7*oneday

            lat = lat1
            while lat <= lat2:
                lon = lon1
                while lon <= lon2:
                    self.get_images_by_region_and_time(s_ts, e_ts, lat, lon)


                    lon += delta_lon
                lat += delta_lat



    def get_images_by_region_and_time(self, s_ts, e_ts, lat, lon):
        params = {
                    'lat' : lat,
                    'lng' : lon, 
                    'min_timestamp' : s_ts,
                    'max_timestamp' : e_ts,
                    'distance' : 5000,
                    'access_token' : self.access_token,
                }

        path = '%s?%s' % (MEDIA_SEARCH, urllib.urlencode(params))
        status, r_headers, content = self.request('GET', host=API_HOST, path=path, headers=self.headers)

        js = json.loads(content)

        if 'pagination' in content:
            pdb.set_trace()

        for image in js['data']:
            self.handle_image(image)

            user = image['user']
            self.handle_user(user)


    def handle_image(self, image):
        image_id = image['id']

        if self.is_seen_before('image', image_id):
            return

        self.write('image', json.dumps(image))

        image_url = image['images']['thumbnail']['url']
        self.download(image_url)


    def handle_user(self, user):
        user_id = user['id']
        if self.is_seen_before('user', user_id):
            return 

        ## get user info
        #path = '%s/%s?access_token=%s' % (USER_INFO, user_id, self.access_token)
        #status, r_headers, content = self.request('GET', host=API_HOST, path=path, headers=self.headers)

        #js = json.loads(content)
        #code = js['meta']['code']
        #if code != 200:
        #    return 

        #user = js['data']
        self.write('user', json.dumps(user))

        # get user's followers
        path = first_path = '%s/%s/followed-by?access_token=%s' % (USER_INFO, user_id, self.access_token)

        while True:
            status, r_headers, content = self.request('GET', host=API_HOST, path=path, headers=self.headers)

            js = json.loads(content)
            code = js['meta']['code']
            if code != 200:
                break 

            for follower in js['data']:
                follower_id = follower['id']

                self.write('follow', '%s %s' % (follower_id, user_id))

                if not self.is_seen_before('user', follower_id):
                    self.write('user', json.dumps(follower))

            if not js.get('pagination'):
                break
            
            cursor = js['pagination']['next_cursor']
            path = '%s&cursor=%s' % (first_path,cursor)


    def is_seen_before(self, typ, item_id):
        if item_id in self.seen[typ]:
            return True
        
        self.seen[typ].add(item_id)
        return False


    def display(self):
        while True:
            print time.ctime()
            print 'Downloaded images:', self.nu['image']
            print 'Downloaded users:', self.nu['user']
            print 'Downloaded friendship:', self.nu['follow']
            
            print '' 
            time.sleep(5)


InstagramCrawler().get_images_by_region('NYC')

