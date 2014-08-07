from task_base import *
from models import *
from crawler import *
import logging

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

class ImageCrawler(Crawler):
  def __init__(self):
    Crawler.__init__(self, [IMG_URL_RE], [], 1, 10, ImageCrawlerThread)
    self.results = {}

  def on_append(self, urls, level, context):
    # TODO: fill results. Group frames by stations
    if level == 0:
      return
    logging.debug("Append %d frames for station %s" % (len(urls), context.name))
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
    # TODO: download images and create blobs
    # TODO: create tree
    # TODO: commit to GitHub
    self.response.set_status(200)
