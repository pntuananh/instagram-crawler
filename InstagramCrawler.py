import httplib, urllib, urllib2
import myjson as json
import time, datetime
import os, thread, glob
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

ENTITY_TYPES = ['image', 'user'] #, 'follow']

MAX_N_ITEM = 10000
TO_FLUSH = 100

SLEEP = 0.75

#regions = {
#        'NYC': {
#            'lat1' : 40.54406959,
#            'lon1' : -74.04716492,
#            'lat2' : 40.89586771,
#            'lon2' : -73.73130798,
#            },
#        }
#
#delta_lat = 0.046
#delta_lon = 0.059

start_time  = '13/11/2013'
end_time    = '13/12/2014'
start_timestamp = int(time.mktime(datetime.datetime.strptime(start_time, "%d/%m/%Y").timetuple()))
end_timestamp = int(time.mktime(datetime.datetime.strptime(end_time, "%d/%m/%Y").timetuple()))

oneday = 86400

def convert(s):
    try: 
        return s.encode('utf-8')
    except:
        return s

class InstagramCrawler():

    def __init__(self, reload=False):
        self.client_id = '9936fc4ff90f4344a2384c35766525c6'
        self.client_secret = '5980acbbca814e4f94c3ff19beef7673'
        self.redirect_uri = 'https://www.ntu.edu.sg/'

        self.seen = {}
        self.nu = {}
        self.files = {}

        self.conn = {}

        print 'Reload:', reload
        for typ in ENTITY_TYPES:
            self.seen[typ] = set()
            self.nu[typ] = 0
            if not reload:
                self.files[typ] = self.open_file(typ, 0)
            else:
                print typ
                max_index = -1
                for filename in glob.glob(DATA_DIR + typ + '[0-9]*.txt'):
                    index = int(filename[-8:-4]) 

                    for line in self.open_file(typ, index, 'r'):
                        try:
                            js =json.loads(line)
                            if typ == 'image':
                                self.seen[typ].add(js['id'])
                            elif typ == 'user':
                                self.seen[typ].add(js['data']['user']['id'])
                        except:
                            pass

                        self.nu[typ] += 1

                    max_index = max(max_index, index)

                if max_index < 0:
                    self.files[typ] = self.open_file(typ, 0)
                else:
                    self.files[typ] = self.open_file(typ, max_index, 'a')

                print max_index
                print self.nu[typ]
                print ''

        self.ferror = open('error.txt', 'w')

        if not os.path.exists(IMAGE_DIR):
            os.makedirs(IMAGE_DIR)

        self.prev_time = time.time()
        self.get_token_access()
        #thread.start_new_thread(self.display, ())


    def request(self, method='GET', host=HOST, path='', headers={}, params={}, data=None):
        self.display()

        if host not in self.conn:
            self.conn[host] = httplib.HTTPSConnection(host)

        conn = self.conn[host]

        if self.access_token:
            params['access_token'] = self.access_token
        if params:
            path = '%s?%s' % (path, urllib.urlencode(params))


        retry = 3
        while retry:
            try:
                if method == 'POST' and data:
                    conn.request(method, path, body=data, headers=headers)
                else:
                    conn.request(method, path, headers=headers)

                r = conn.getresponse()

                res_headers = {k:v for k,v in r.getheaders()}
                content = r.read()

                time.sleep(SLEEP)
                
                return r.status, r.reason, res_headers, content
            except Exception, e:
                print e
                self.log_error(str(e))
                retry -= 1
                time.sleep(5)
                del self.conn[host]
                #self.conn[host] = httplib.HTTPSConnection(host)
                self.get_token_access()

        return -1, '', '', ''


    def get_token_access(self):
        print 'Requesting token access...',

        self.access_token = None
        path = '/oauth/authorize/?client_id=%s&redirect_uri=%s&response_type=code' % (self.client_id, urllib.quote(self.redirect_uri)) 
        self.headers = {}

        status, reason, r_headers, content = self.request('GET', path=path, headers=self.headers)

        url = r_headers['location']
        path = url[url.find('/', 8):]

        csrftoken = r_headers['set-cookie'].split('csrftoken=')[1].split(';')[0]
        self.headers['Cookie'] = 'csrftoken=%s' % csrftoken

        status, reason, r_headers, content = self.request('GET', path=path, headers=self.headers)

        mid = r_headers['set-cookie'].split("mid=")[1].split(";")[0]
        self.headers['Cookie'] += '; mid=%s' % mid

        self.headers['Referer'] = url 
        data = 'csrfmiddlewaretoken=%s&username=pntuananh&password=swordfish' % csrftoken

        status, reason, r_headers, content = self.request('POST', path=path, headers=self.headers, data=data)
        
        url = r_headers['location']
        path = url[url.find('/', 8):]
        self.headers['Referer'] = url 

        cookie = r_headers['set-cookie']
        sessionid = cookie.split('sessionid=')[1].split(';')[0]
        self.headers['Cookie'] += '; sessionid=%s' % sessionid

        status, reason, r_headers, content = self.request('GET', path=path, headers=self.headers)

        path = '/oauth/authorize/?client_id=%s&redirect_uri=%s&response_type=code' % (self.client_id, urllib.quote(self.redirect_uri))
        data = 'csrfmiddlewaretoken=%s&allow=Authorize' % csrftoken

        status, reason, r_headers, content = self.request('POST', path=path, headers=self.headers, data=data)

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

        status, reason, r_headers, content = self.request('POST', host=API_HOST, path=path, data=urllib.urlencode(client_params))

        js =  json.loads(content)

        print 'Done'

        self.access_token = js['access_token']


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
            directory = IMAGE_DIR + filename[:3]
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


    #def get_images_by_region(self,reg):
    #    thread.start_new_thread(self.display, ())

    #    start_timestamp = int(time.mktime(datetime.datetime.strptime(start_time, "%d/%m/%Y").timetuple()))
    #    end_timestamp = int(time.mktime(datetime.datetime.strptime(end_time, "%d/%m/%Y").timetuple()))

    #    geo = regions[reg]
    #    lat1 = geo['lat1']
    #    lon1 = geo['lon1']
    #    lat2 = geo['lat2']
    #    lon2 = geo['lon2']

    #    for e_ts in xrange(end_timestamp, start_timestamp,-7*oneday):
    #        s_ts = e_ts - 7*oneday

    #        lat = lat1
    #        while lat <= lat2:
    #            lon = lon1
    #            while lon <= lon2:
    #                self.get_images_by_region_and_time(s_ts, e_ts, lat, lon)


    #                lon += delta_lon
    #            lat += delta_lat


    #def get_images_by_region_and_time(self, s_ts, e_ts, lat, lon):
    #    params = {
    #                'lat' : lat,
    #                'lng' : lon, 
    #                'min_timestamp' : s_ts,
    #                'max_timestamp' : e_ts,
    #                'distance' : 5000,
    #                'access_token' : self.access_token,
    #            }

    #    path = '%s?%s' % (MEDIA_SEARCH, urllib.urlencode(params))
    #    status, reason, r_headers, content = self.request('GET', host=API_HOST, path=path, headers=self.headers)

    #    js = json.loads(content)

    #    if 'pagination' in content:
    #        pdb.set_trace()

    #    for image in js['data']:
    #        if image['type'] != 'image': continue

    #        self.handle_image(image)

    #        user = image['user']
    #        self.handle_user(user)


    def get_images_by_venues(self, list_foursquare_venues):
        for foursquare_image_id in list_foursquare_venues:
            instagram_image_id = self.get_instagram_image_id(foursquare_image_id)

            path = '%s/%s/media/recent' % (LOCATION_SEARCH, instagram_image_id)
            params = {
                    'min_timestamp' : start_timestamp,
                    'max_timestamp' : end_timestamp,
                    }

            while True:
                status, reason, r_headers, content = self.request('GET', host=API_HOST, path=path, params=params, headers=self.headers)

                try:
                    js = json.loads(content)
                except Exception, e:
                    self.log_error(str(e))
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


    def get_instagram_image_id(self, foursquare_image_id):
        path = '%s/search' % LOCATION_SEARCH
        params = {'foursquare_v2_id' : foursquare_image_id}

        status, reason, r_headers, content = self.request('GET', host=API_HOST, path=path, params=params, headers=self.headers)

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
            status, reason, r_headers, content = self.request('GET', host=API_HOST, path=path, params=params, headers=self.headers)

            try:
                js = json.loads(content)
            except Exception, e:
                self.log_error(str(e))
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

        
        ## get user info
        #path = '%s/%s' % (USER_INFO, user_id)
        #status, reason ,r_headers, content = self.request('GET', host=API_HOST, path=path, headers=self.headers)

        #js = json.loads(content)
        #code = js['meta']['code']
        #if code != 200:
        #    return 

        #user = js['data']
        #self.write('user', json.dumps(user))

        ## get user's followers
        #path = first_path = '%s/%s/follows' % (USER_INFO, user_id)

        #while True:
        #    status, reason, r_headers, content = self.request('GET', host=API_HOST, path=path, headers=self.headers)

        #    try:
        #        js = json.loads(content)
        #    except:
        #        pdb.set_trace()

        #    code = js['meta']['code']
        #    if code != 200:
        #        break 

        #    for followee in js['data']:
        #        followee_id = followee['id']

        #        self.write('follow', '%s %s' % (user_id, followee_id))

        #        if not self.is_seen_before('user', followee_id):
        #            self.write('user', json.dumps(followee))

        #    if not js.get('pagination'):
        #        break
        #    
        #    cursor = js['pagination']['next_cursor']
        #    path = '%s&cursor=%s' % (first_path,cursor)

    def is_seen_before(self, typ, item_id):
        if item_id in self.seen[typ]:
            return True
        
        self.seen[typ].add(item_id)
        return False


    def log_error(self, msg):
        self.ferror.write('%s : %s\n' % (time.ctime(), msg))
        self.ferror.flush()
        os.fsync(self.ferror.fileno())


    def display(self):
        curr_time = time.time()
        if curr_time - self.prev_time > 5:
            print time.ctime()
            print 'Downloaded images:', len(self.seen['image']) #self.nu['image']
            print 'Downloaded users:', len(self.seen['user']) #self.nu['user']
            #print 'Downloaded friendship:', self.nu['follow']
            
            print '' 

            self.prev_time = curr_time


if __name__ == "__main__":        
    reload = len(sys.argv) > 1 and sys.argv[1] == '1'
    #InstagramCrawler(reload=reload).get_images_by_region('NYC')

    list_foursquare_venues = []
    for line in open('VenueInfo_Coordinate.txt'):
        venue = line.split('\t')[0]
        list_foursquare_venues += [venue]

    InstagramCrawler(reload=reload).get_images_by_venues(list_foursquare_venues)

    sys.exit()
