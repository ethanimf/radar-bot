from task_base import *
from models import *
from crawler import *
from datetime import datetime
from deployers import *
import logging
import re
MAX_FRAME_PER_CRON = 5

class ImageCrawlerThread(CrawlerThread):
  def init(self):
    self.max_frame_count = MAX_FRAME_PER_CRON
  def should_walk(self, url, urls, context):
    if len(urls) >= self.max_frame_count:
      return False
    # TODO: check time stamp
    return True

IMG_URL_RE = "javascript:view_text_img\((\'.*?\'),(\'.*?\'),'','','','',(\'.*?\'),'',(\'.*?\'),'',''\)"
IMG_ENLARGE_RE = "(http:\/\/image\.weather\.gov\.cn)(.*)"

# 1: Url
# 2: Station ID
# 3: YYYYMMDDHHmm or "%Y%m%d%H%M"
# 4: YYYY
# 5: MM
# 6: DD
# 7: HHmm
TIME_STAMP_RE = "'?(\/product\/\d{4}\/\d{6}\/\d{8}\/RDCP\/SEVP_AOC_RDCP_SLDAS_EBREF_AZ(\d{4})_L88_PI_((\d{4})(\d{2})(\d{2})(\d{4})).*)'?"
TIME_STAMP_FORMAT = "%Y%m%d%H%M"

def extract_frame_info(script_url):
  url = script_url
  # Extract url with ''
  m = re.match(IMG_URL_RE, url)
  if not m:
    m = re.match(IMG_ENLARGE_RE, url)
  if not m:
    return
  url = m.group(2)
  # Extract url and date time
  m = re.match(TIME_STAMP_RE, url)
  url = m.group(1)
  timestamp = m.group(3)
  time = datetime.strptime(timestamp, TIME_STAMP_FORMAT)
  station_id = m.group(2)
  return (url, time, station_id, timestamp)

class ImageCrawler(Crawler):
  def __init__(self):
    Crawler.__init__(self, [IMG_URL_RE, IMG_ENLARGE_RE], [], 1, 10, ImageCrawlerThread)
    self.results = {}
    self.new_frame_count = 0

  def on_append(self, urls, level, context):
    # TODO: fill results. Group frames by stations
    if level == 0:
      return
    if len(urls) == 0:
      return
    for url in urls:
      r = extract_frame_info(url)
      if not r:
        continue
      u = r[0]
      t = r[1]
      id = r[2]
      station = self.stations.get(id)
      if station == None:
        logging.warning("Unknown station id %s from %s" % (id, context))
        continue
      # New frame
      if station.last_update == None or station.last_update < t:
        self.new_frame_count += 1
        if not self.results.has_key(id):
          self.results[id] = []
        frame = Frame.create_from_frame_info(station, r)
        self.results[id].append(frame)

      # Keep track of newst frame in this update
      # and set to station.last_update later
      if station._this_update < t:
        station._this_update = t


class FrameTaskHandler(TaskHandler):
  def get_name(self):
    return 'frame'

  def run_task(self):
    # Query station list
    query = Station.create_query_for_all()
    station_count = query.count()
    logging.info("Find %d stations in store" % (station_count))
    if station_count == 0:
      logging.warning("No stations found in datastore. Didn't run /tasks/station?")
      self.response.set_status(200)
      return
    # TODO: get frame image urls
    tasks = []
    stations = {}
    for station in query.iter():
      #logging.debug("Station: %s, Url: %s" % (station.name, station.url))
      tasks.append((station.url, station))
      stations[station.station_id] = station
    # TODO: spawn frame crawlers for each frame (with last_updated and max_frame_count)
    logging.info("Start frame crawler")
    crawler = ImageCrawler()
    crawler.stations = stations
    crawler.walk_with_context(tasks)
    if crawler.fail_count > 0:
      logging.warning("%d tasks failed" % (crawler.fail_count))
    logging.info("Find %d frames, %d new since last update" % (len(crawler.urls), crawler.new_frame_count))
    # TODO: download images and create blobs
    # TODO: create tree
    # TODO: commit to GitHub
    # NOTE: this will cause thousands+ datastore write ops per cron,
    #       which would burn through all 50k free quota very quickly.
    #       We'll use GitHub API to determine frame lists after commit
    # Update frames
    # all_frames = reduce(lambda f1, f2: f1 + f2, crawler.results.values())
    # logging.info("Write %d frames to datastore" % len(all_frames))
    # ndb.put_multi(all_frames)
    # Update time
    logging.info("Start deploying")
    deployer = GitHubDeployer(crawler.results)
    deployer.deploy()
    logging.info("Update stations in datastore")
    for station in stations.values():
      if station._this_update != None:
        station.last_update = station._this_update
    # Put changes
    ndb.put_multi(stations.values())
    self.response.set_status(200)
