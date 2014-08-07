from google.appengine.ext import ndb
import re

MODEL_VERSION = 3

def get_parent_key(version = MODEL_VERSION):
  return ndb.Key('Station', "v%d" % (version))

URL_RE = "http:\/\/www\.nmc\.gov\.cn\/publish\/radar\/(stations-)?(.*)\.htm"
IMG_RE = ""
def get_name_from_url(url):
  m = re.match(URL_RE, url)
  if m:
    return m.group(2)
  return ''

class Station(ndb.Model):
  url = ndb.StringProperty()
  name = ndb.StringProperty()
  station_id = ndb.StringProperty()
  last_update = ndb.DateTimeProperty()
  last_commit = ndb.StringProperty()

  @classmethod
  def create_query_for_all(cls):
    parent = get_parent_key()
    return cls.query(ancestor = parent)
  @classmethod
  def create_or_update_from_url(cls, url, id_table):
    name = get_name_from_url(url)
    parent = get_parent_key()
    id = id_table.get(name)
    # Possible cases:
    # 1. No data
    # 2. Capital cities of provinces
    if id == None:
      return
    station = cls.get_or_insert(id, parent = parent)
    station.url = url
    station.name = name
    station.station_id = id
    station.put()
    return station
