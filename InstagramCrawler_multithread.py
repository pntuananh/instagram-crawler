import httplib, urllib, urllib2, socket
import myjson as json
import ujson
import time, datetime
import os, thread, threading, glob
import sys
from collections import deque
import pdb


HOST         = 'instagram.com'
API_HOST     = 'api.instagram.com'
MEDIA_SEARCH = '/v1/media/search'
USER_INFO    = '/v1/users'
LOCATION_SEARCH = '/v1/locations'

DATA_DIR = 'data/'
IMAGE_DIR = DATA_DIR + 'images/'

ENTITY_TYPES = ['image', 'user'] 

MAX_N_ITEM = 10000
TO_FLUSH = 100

N_CLIENTS = 3

SLEEP = 0.2
TIMEOUT = 60
socket.setdefaulttimeout(TIMEOUT)

start_time  = '13/11/2013'
end_time    = '13/11/2014'

start_day = datetime.datetime.strptime(start_time, "%d/%m/%Y")
end_day = datetime.datetime.strptime(end_time, "%d/%m/%Y")

start_timestamp = int(time.mktime(start_day.timetuple()))
end_timestamp = int(time.mktime(end_day.timetuple()))

oneday = 86400

def convert(s):
    try: 
        return s.encode('utf-8')
    except:
        return s

class InstagramCrawler():

    def __init__(self, reload=False):
        self.read_clients('clients.txt')

        self.seen = {}
        self.mtx_seen = {}
        self.nu = {}
        self.files = {}

        self.conn = {}
        self.venues = 0

        print 'Reload:', reload
        for typ in ENTITY_TYPES:
            self.seen[typ] = set()
            self.mtx_seen[typ] = threading.Lock()
            self.nu[typ] = 0
            if not reload:
                self.files[typ] = self.open_file(typ, 0)
            else:
                print 'Reloading %s...' % typ
                max_index = -1
                for filename in glob.glob(DATA_DIR + typ + '[0-9]*.txt'):
                    index = int(filename[-8:-4]) 

                    for line in self.open_file(typ, index, 'r'):
                        try:
                            #js =json.loads(line)
                            js = ujson.loads(line)
                            if typ == 'image':
                                self.seen[typ].add(js['id'])
                            elif typ == 'user':
                                users = js['data']
                                if users:
                                    self.seen[typ].add(users[0]['user']['id'])
                            self.nu[typ] += 1

                            if self.nu[typ] % 1000 == 0:
                                print '\r%d' % self.nu[typ],
                        except:
                            pass

                    max_index = max(max_index, index)

                if max_index < 0:
                    self.files[typ] = self.open_file(typ, 0)
                else:
                    self.files[typ] = self.open_file(typ, max_index, 'a')

                print ''
                print max_index
                print self.nu[typ]
                print ''

        self.ferror = open('error.txt', 'w')
        self.url_file = open('url.txt', 'w')
        self.n_url = 0

        self.mtx_io = threading.Lock()

        if not os.path.exists(IMAGE_DIR):
            os.makedirs(IMAGE_DIR)

        self.prev_time = time.time()

        for idx in range(len(self.clients)):
            self.get_token_access(idx)

        self.client_queue = deque(range(len(self.clients)))
        self.mtx_client_queue = threading.Lock()

        self.queue = None
        self.mtx_queue = threading.Lock()

        #thread.start_new_thread(self.display, ())

        #self.current_client = 0

    def read_clients(self, filename):
        f = open(filename)
        s = f.read()
        f.close()

        parts = s.split('\n\n')
        self.clients = []
        for p in parts:
            cl = {}
            for line in p.split('\n'):
                if not line: continue
                if line[0] == '#': continue
                key, value = line.split(' : ')
                cl[key] = value

            if not cl: continue

            cl['access_token'] = ''
            cl['conn'] = None
            cl['headers'] = {}

            self.clients.append(cl)


    def get_client(self):
        while True:
            self.mtx_client_queue.acquire()
            if not self.client_queue:
                self.mtx_client_queue.release()
                time.sleep(1)
                continue

            idx_client = self.client_queue.popleft()
            self.mtx_client_queue.release()
            break

        return idx_client


    def add_client(self, idx_client):
        self.mtx_client_queue.acquire()
        self.client_queue.append(idx_client)
        self.mtx_client_queue.release()


    def request(self, path='', params={}):
        #self.display()

        idx_client = self.get_client()
        retry = 3
        client = self.clients[idx_client]

        status, reason, r_headers, content =  -1, '', {}, ''

        while retry:
            conn         = client['conn']
            access_token = client['access_token']
            headers      = client['headers']

            if access_token:
                params['access_token'] = access_token
            if params:
                path = '%s?%s' % (path, urllib.urlencode(params))

            self.log_url(path)

            try:
                conn.request('GET', path, headers=headers)

                r = conn.getresponse()

                status = r.status
                reason = r.reason
                r_headers = {k:v for k,v in r.getheaders()}
                content = r.read()

                time.sleep(SLEEP)

                break
            except Exception, e:
                print e
                u = '%s?%s' % (path, urllib.urlencode(params))
                self.log_error('%s - %s' % (u, str(e)))
                retry -= 1
                time.sleep(5)

                self.get_token_access(idx_client)


        #self.current_client = (self.current_client+1) % len(self.clients)
        self.add_client(idx_client)

        return status, reason, headers, content 


    def get_token_access(self, cl_idx):
        print 'Requesting token access...',

        client = self.clients[cl_idx]
        client['access_token'] = None

        conn = httplib.HTTPSConnection(HOST, timeout=TIMEOUT)
        path = '/oauth/authorize/?client_id=%s&redirect_uri=%s&response_type=code' % (client['client_id'], urllib.quote(client['redirect_uri'])) 
        headers = client['headers'] = {}

        conn.request('GET', path)
        r = conn.getresponse()
        r.read()

        url = r.getheader('location')
        path = url[url.find('/', 8):]

        csrftoken = r.getheader('set-cookie').split('csrftoken=')[1].split(';')[0]
        headers['Cookie'] = 'csrftoken=%s' % csrftoken

        conn.request('GET', path, headers=headers)
        r = conn.getresponse()
        r.read()

        mid = r.getheader('set-cookie').split("mid=")[1].split(";")[0]
        headers['Cookie'] += '; mid=%s' % mid

        headers['Referer'] = url 
        data = 'csrfmiddlewaretoken=%s&username=%s&password=%s' % (csrftoken, client['username'], client['password'])

        conn.request('POST', path, headers=headers, body=data)
        r = conn.getresponse()
        r.read()
        
        url = r.getheader('location')
        path = url[url.find('/', 8):]
        headers['Referer'] = url 

        cookie = r.getheader('set-cookie')
        sessionid = cookie.split('sessionid=')[1].split(';')[0]
        headers['Cookie'] += '; sessionid=%s' % sessionid

        conn.request('GET', path, headers=headers)
        r = conn.getresponse()
        r.read()

        path = '/oauth/authorize/?client_id=%s&redirect_uri=%s&response_type=code' % (client['client_id'], urllib.quote(client['redirect_uri']))
        data = 'csrfmiddlewaretoken=%s&allow=Authorize' % csrftoken

        conn.request('POST', path, headers=headers, body=data)
        r = conn.getresponse()
        r.read()

        url = r.getheader('location')
        code = url.split('code=')[1]

        client_params = {
            "client_id"     : client['client_id'],
            "client_secret" : client['client_secret'],
            "redirect_uri"  : client['redirect_uri'],
            "grant_type"    : "authorization_code",
            'code'          : code,
        }

        path = '/oauth/access_token'

        conn = client['conn'] = httplib.HTTPSConnection(API_HOST, timeout=TIMEOUT)
        conn.request('POST', path, body=urllib.urlencode(client_params))

        r = conn.getresponse()
        content = r.read()

        js =  json.loads(content)

        print 'Done'

        access_token = client['access_token'] = js['access_token']
        print access_token


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
            except Exception, e:
                retry -= 1

                self.log_error('%s - %s' % (url, str(e)))

        if image:
            self.mtx_io.acquire()
            directory = IMAGE_DIR + filename[:2]
            if not os.path.exists(directory):
                os.makedirs(directory)
            
            filename = '%s/%s' % (directory,filename)
            try:
                open(filename, 'wb').write(image)
            except:
                pass

            self.mtx_io.release()


    def write(self, typ, item):
        self.mtx_io.acquire()

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

        self.mtx_io.release()


    def get_venue(self):
        if not self.queue:
            return None

        self.mtx_queue.acquire()
        foursquare_venue_id, start_dt, end_dt = self.queue.popleft()
        self.venues += 1
        self.mtx_queue.release()

        start_ts = int(time.mktime(start_dt.timetuple()))
        end_ts = int(time.mktime(end_dt.timetuple()))

        end_dt = start_dt
        start_dt = max(start_dt - datetime.timedelta(days=30), start_day)

        if end_dt > start_day:
            self.mtx_queue.acquire()
            self.queue.append((foursquare_venue_id, start_dt, end_dt))
            self.mtx_queue.release()
            
        return foursquare_venue_id, start_ts, end_ts


    def create_queue(self, list_foursquare_venues):
        self.queue = deque()

        end_dt = end_day
        start_dt = end_day - datetime.timedelta(days=30)

        for foursquare_venue_id in list_foursquare_venues:
            self.queue.append((foursquare_venue_id, start_dt, end_dt))


    def get_images_by_venues(self, list_foursquare_venues):
        self.create_queue(list_foursquare_venues)

        for i in range(N_CLIENTS):
            thread.start_new_thread(self.child_thread, (i,))

        self.display()


    def child_thread(self, i):
        while True:
            ven = self.get_venue()

            if not ven: break

            foursquare_venue_id, start_ts, end_ts = ven

            instagram_venue_id = self.get_instagram_venue_id(foursquare_venue_id)

            path = '%s/%s/media/recent' % (LOCATION_SEARCH, instagram_venue_id)
            params = {
                    'min_timestamp' : start_ts,
                    'max_timestamp' : end_ts,
                    }

            while True:
                status, reason, r_headers, content = self.request(path=path, params=params)

                if status != 200: break
                try:
                    js = json.loads(content)
                except Exception, e:
                    u = '%s?%s' % (path, urllib.urlencode(params))
                    self.log_error('%s - %s' % (u, str(e)))
                    break

                code = js['meta']['code']
                if code != 200:
                    break

                for image in js['data']:
                    if image['type'] != 'image': continue

                    if not self.handle_image(image):
                        continue

                    user = image['user']
                    self.handle_user(user)


                if not js.get('pagination'):
                    break
                
                next_max_id = js['pagination']['next_max_id']
                #next_path = '%s&max_id=%s' % (path, next_max_id)
                params['max_id'] = next_max_id



    def get_instagram_venue_id(self, foursquare_venue_id):
        path = '%s/search' % LOCATION_SEARCH
        params = {'foursquare_v2_id' : foursquare_venue_id}

        status, reason, r_headers, content = self.request(path=path, params=params)

        if status != 200:
            return ''

        js = json.loads(content)
        if not js['data']:
            return ''

        return js['data'][0]['id']


    def handle_image(self, image):
        image_id = image['id']

        if self.is_seen_before('image', image_id):
            return False

        self.write('image', json.dumps(image))

        image_url = image['images']['low_resolution']['url']
        self.download(image_url)

        return True


    def handle_user(self, user):
        user_id = user['id']
        if self.is_seen_before('user', user_id):
            return False

        path = '%s/%s/media/recent' % (USER_INFO, user_id)
        params = {
                'min_timestamp' : start_timestamp,
                'max_timestamp' : end_timestamp,
                }

        while True:
            status, reason, r_headers, content = self.request(path=path, params=params)

            if status != 200: 
                break
         
            try:
                js = json.loads(content)
            except Exception, e:
                u = '%s?%s' % (path, urllib.urlencode(params))
                self.log_error('%s - %s' % (u, str(e)))
                break

            code = js['meta']['code']
            if code != 200:
                break

            self.write('user', json.dumps(js))

            if not js.get('pagination'):
                break
            
            next_max_id = js['pagination']['next_max_id']
            #next_path = '%s&max_id=%s' % (path, next_max_id)
            params['max_id']= next_max_id

        return True

        
    def is_seen_before(self, typ, item_id):
        self.mtx_seen[typ].acquire()
        ret = False
        if item_id in self.seen[typ]:
            ret = True
        
        self.seen[typ].add(item_id)
        self.mtx_seen[typ].release()

        return ret


    def log_error(self, msg):
        self.mtx_io.acquire()

        self.ferror.write('%s : %s\n' % (time.ctime(), msg))
        self.ferror.flush()
        os.fsync(self.ferror.fileno())

        self.mtx_io.release()


    def log_url(self, url):
        self.mtx_io.acquire()

        if self.n_url == 1000:
            self.url_file.close()
            self.url_file = open('url.txt', 'w')
            self.n_url = 0

        self.n_url += 1
        self.url_file.write('%s - %s\n' % (time.ctime(), url))
        self.url_file.flush()

        self.mtx_io.release()


    def display(self):
        while True:
            curr_time = time.time()
            if curr_time - self.prev_time > 5:
                print time.ctime()
                print 'Downloaded images:', len(self.seen['image']) #self.nu['image']
                print 'Downloaded users:', len(self.seen['user']) #self.nu['user']
                print 'Downloaded venues:', self.venues #self.nu['user']
                #print 'Downloaded friendship:', self.nu['follow']
                
                print '' 
                self.prev_time = curr_time

            time.sleep(5)



if __name__ == "__main__":        
    reload = len(sys.argv) > 1 and sys.argv[1] == '1'
    #InstagramCrawler(reload=reload).get_images_by_region('NYC')

    list_foursquare_venues = []
    for line in open('VenueInfo_Coordinate.txt'):
        venue = line.strip('\n').split('\t')[0]
        list_foursquare_venues += [venue]

    InstagramCrawler(reload=reload).get_images_by_venues(list_foursquare_venues)

    sys.exit()
