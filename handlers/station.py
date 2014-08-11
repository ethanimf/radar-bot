from task_base import *
from crawler import *
from models import *
from deployers import *
import xml.etree.ElementTree as ET
import json
#from google.appengine.api import logservice
#import logging

start_url = 'http://www.nmc.gov.cn/publish/radar/beijing.htm'
STATION_RE = "(http:\\/\\/www\\.nmc\\.gov\\.cn\\/publish\\/radar\\/)"
IMAGE_RE = "http:\/\/image\.weather\.gov\.cn(.*)"
# 1: Url
# 2: Station ID
# 3: YYYYMMDDHHmm or "%Y%m%d%H%M"
# 4: YYYY
# 5: MM
# 6: DD
# 7: HHmm
TIME_STAMP_RE = "(\/product\/\d{4}\/\d{6}\/\d{8}\/RDCP\/SEVP_AOC_RDCP_SLDAS_EBREF_AZ(\d{4})_L88_PI_((\d{4})(\d{2})(\d{2})(\d{4})).*)"
white_rules = [STATION_RE]
black_rules = ['(http:\\/\\/www\\.nmc\\.gov\\.cn\\/publish\\/radar\\/)(chinaall|stationindex)\\.htm']


class StationCrawlerThread(CrawlerThread):
  def init(self):
    if not hasattr(self.crawler, 'station_id_table'):
      self.crawler.station_id_table = {}

  def should_walk(self, url, urls, context):
    if re.match(URL_RE, url):
      return True
    m = re.match(IMAGE_RE, url)
    if m:
      m = re.match(TIME_STAMP_RE, m.group(1))
      if m:
        id = m.group(2)
        name = get_name_from_url(context)
        logging.debug("Station id %s for %s" % (m.group(2), name))
        if id != None:
          self.crawler.station_id_table[name] = id
    return False

class StationTaskHandler(TaskHandler):
  def get_name(self):
    return 'station'

  def read_station_info(self):
    info = {}
    with open("data/fusion.json") as json_f:
      info = json.load(json_f)
    logging.info("Load total %d station inforamtion" % (len(info)))
    return info

  def run_task(self):
    logging.info("Loading station information")
    station_info = self.read_station_info()
    logging.info("Create crawler")
    crawler = Crawler(white_rules, black_rules, 3, thread_klass = StationCrawlerThread)
    logging.info("Start walking")
    crawler.walk([start_url])
    logging.info("Walking finished")
    stations = []
    location_known_count = 0
    for url in crawler.urls:
      station = Station.create_or_update_from_url(url, crawler.station_id_table)
      if not station:
        continue
      if station_info.has_key(station.station_id):
        i = station_info[station.station_id]
        station.location = ndb.GeoPt("%s, %s" % (i['lat'], i['lng']))
        station.frame_range = int(i['range'])
        # BUG: location_known_count > len(station_info), possible dupilications
        location_known_count += 1
      stations.append(station)
    ndb.put_multi(stations)
    # Deploy
    logging.info("Deployer station.json")
    deployer = GitHubDeployer(stations, type = 'station')
    deployed = deployer.deploy()
    logging.info("Deployed: %s" % (deployed))
    # Log
    if crawler.fail_count > 0:
      logging.warning("%d tasks failed" % (crawler.fail_count))
    logging.info("Found %d stations, %d with known ID, %d with known location" % (len(crawler.urls), len(crawler.station_id_table), location_known_count))
    if deployed:
      self.response.set_status(200)
    else:
      self.response.set_status(500)
