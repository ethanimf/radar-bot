from task_base import *
from models import *
from crawler import *
from datetime import datetime
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

# 1: Url
# 2: Station ID
# 3: YYYYMMDDHHmm or "%Y%m%d%H%M"
# 4: YYYY
# 5: MM
# 6: DD
# 7: HHmm
TIME_STAMP_RE = "'(\/product\/\d{4}\/\d{6}\/\d{8}\/RDCP\/SEVP_AOC_RDCP_SLDAS_EBREF_AZ(\d{4})_L88_PI_((\d{4})(\d{2})(\d{2})(\d{4})).*)'"
TIME_STAMP_FORMAT = "%Y%m%d%H%M"

def extract_url_and_time(script_url):
  url = script_url
  # Extract url with ''
  m = re.match(IMG_URL_RE, url)
  url = m.group(2)
  # Extract url and date time
  m = re.match(TIME_STAMP_RE, url)
  url = m.group(1)
  timestamp = m.group(3)
  time = datetime.strptime(timestamp, TIME_STAMP_FORMAT)
  return (url, time)

class ImageCrawler(Crawler):
  def __init__(self):
    Crawler.__init__(self, [IMG_URL_RE], [], 1, 10, ImageCrawlerThread)
    self.results = {}
    self.new_frame_count = 0

  def on_append(self, urls, level, context):
    # TODO: fill results. Group frames by stations
    if level == 0:
      return
    if len(urls) == 0:
      return
    last_update = datetime.min
    for url in urls:
      r = extract_url_and_time(url)
      u = r[0]
      t = r[1]
      # New frame
      if context.last_update == None or context.last_update < last_update:
        self.new_frame_count += 1
      # Find newest frame
      if last_update < t:
        last_update = t
    # Update station record
    if context.last_update == None or context.last_update < last_update:
      context.last_update = last_update
    #logging.debug("URL: %s Timestamp: %s" % (r[0], r[1].strftime("%a %b %d %H:%M:%S %Y")))
    #logging.debug("Append %d frames for station %s" % (len(urls), context.name))
    pass

class FrameTaskHandler(TaskHandler):
  def get_name(self):
    return 'frame'

  def run_task(self):
    # Query station list
    query = Station.create_query_for_all()
    logging.info("Find %d stations in store" % (query.count()))
    # TODO: get frame image urls
    tasks = []
    for station in query.iter():
      #logging.debug("Station: %s, Url: %s" % (station.name, station.url))
      tasks.append((station.url, station))
    # TODO: spawn frame crawlers for each frame (with last_updated and max_frame_count)
    logging.info("Start frame crawler")
    crawler = ImageCrawler()
    crawler.walk_with_context(tasks)
    logging.info("Find %d frames" % (len(crawler.urls)))
    logging.info("%d new frames since last update" % (crawler.new_frame_count))
    # TODO: download images and create blobs
    # TODO: create tree
    # TODO: commit to GitHub
    # Put changes
    for task in tasks:
      task[1].put()
    self.response.set_status(200)
