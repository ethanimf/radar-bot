from google.appengine.ext import ndb
import re
import logging

class Frame(ndb.Model):
  url = ndb.StringProperty()
  time = ndb.DateTimeProperty()
  station_id = ndb.StringProperty()
  blob = ndb.StringProperty()
  tree = ndb.StringProperty()
  commit = ndb.StringProperty()

  def get_file_name(self):
    m = re.match(".*/(.*)\?.*", self.url)
    return m.group(1)

  @classmethod
  def create_from_frame_info(cls, station, info, put_now = False):
    url = info[0]
    time = info[1]
    station_id = info[2]
    time_str = info[3]
    if station_id != station.station_id:
      logging.error("Expected station id %s, got %s for %s" % (station.station_id, station_id, url))
      return

    frame = Frame(parent = station.key)
    frame.url = url
    frame.time = time
    frame.station_id = station_id
    if put_now:
      frame.put()
    return frame
