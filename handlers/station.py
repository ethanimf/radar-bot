from task_base import *
from crawler import *
from models import *
#from google.appengine.api import logservice
#import logging

start_url = 'http://www.nmc.gov.cn/publish/radar/beijing.htm'
white_rules = ['(http:\\/\\/www\\.nmc\\.gov\\.cn\\/publish\\/radar\\/)']
black_rules = ['(http:\\/\\/www\\.nmc\\.gov\\.cn\\/publish\\/radar\\/)(chinaall|stationindex)\\.htm']

class StationTaskHandler(TaskHandler):
  def get_name(self):
    return 'station'

  def run_task(self):
    logging.info("Create crawler")
    crawler = Crawler(white_rules, black_rules)
    logging.info("Start walking")
    crawler.walk([start_url])
    logging.info("Walking finished")
    for url in crawler.urls:
      Station.create_or_update_from_url(url)
    logging.info("Found %d stations" % (len(crawler.urls)))
    #logservice.flush()
    self.response.set_status(200)
    #self.response.write("Found %d stations" % (len(crawler.urls)))
