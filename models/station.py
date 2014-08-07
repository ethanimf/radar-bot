from google.appengine.ext import ndb
import re

MODEL_VERSION = 1

def get_parent_key(version = MODEL_VERSION):
  return ndb.Key('Station', "v%d" % (version))

URL_RE = "http:\/\/www\.nmc\.gov\.cn\/publish\/radar\/(stations-)?(.*)\.htm"

def get_name_from_url(url):
  m = re.match(URL_RE, url)
  if m:
    return m.group(2)
  return ''

class Station(ndb.Model):
  url = ndb.StringProperty()
  name = ndb.StringProperty()
  last_update = ndb.DateTimeProperty()
  last_commit = ndb.StringProperty()

  @classmethod
  def create_or_update_from_url(cls, url):
    name = get_name_from_url(url)
    parent = get_parent_key()
    station = cls.get_or_insert(name, parent = parent)
    station.url = url
    station.name = name
    station.put()
    return station
