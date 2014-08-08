import re
import sys
import urllib2
import urlparse
from cgi import escape
sys.path.insert(0, 'lib')
from bs4 import BeautifulSoup
import logging
import time
import Queue
import threading

__all__ = ['Fetcher', 'Crawler', 'CrawlerThread']

AGENT = "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/36.0.1985.125 Safari/537.36"
class Fetcher(object):
  def __init__(self, url):
    self.url = url
    self.urls = []

  def __getitem__(self, x):
    return self.urls[x]

  def _addHeaders(self, request):
    request.add_header("User-Agent", AGENT)

  def open(self):
    url = self.url
    try:
      request = urllib2.Request(url)
      handle = urllib2.build_opener()
    except IOError:
      return None
    return (request, handle)

  def fetch(self):
    try:
      request, handle = self.open()
      self._addHeaders(request)
      if handle:
        content = unicode(handle.open(request, timeout=10).read(), "utf-8",
          errors="replace")
        soup = BeautifulSoup(content)
        tags = soup('a')
        for tag in tags:
          href = tag.get("href")
          if href is not None:
            url = urlparse.urljoin(self.url, escape(href))
            if url not in self:
              self.urls.append(url)
    except Exception as e:
      logging.error("Error when fetching: %s" % (e))
      return False
    return True

class CrawlerThread(threading.Thread):
  def __init__(self, crawler, id, max_retry = 3):
    threading.Thread.__init__(self)
    self.crawler = crawler
    self.id = id
    self.idle = True
    self.init()
    self.max_retry = max_retry
    self.current_retry = 0

  def init(self):
    pass

  def run(self):
    crawler = self.crawler
    logging.debug("Thread %d started" % (self.id))
    while not crawler.shouldExit:
      if not crawler.queue.empty():
        self.idle = False
        task = crawler.queue.get()
        self.current_retry = 0
        succ = False
        while self.current_retry <= self.max_retry:
          if self.current_retry > 0:
            logging.warning("Failed last time, retrying (%d/%d)" % (self.current_retry, self.max_retry))
          # walk
          succ = self.walk(task)
          if succ:
            break
          self.current_retry += 1
        if not succ:
          logging.error("Fail to run %s" % (task[0]))
          crawler.queue.task_done()
          crawler.fail_count += 1
        # done
        #crawler.queue.task_done()
        self.idle = True
      time.sleep(1)
    logging.debug("Thread %d terminated" % (self.id))

  def match_rule_list(self, url, rules):
      #url = url.encode('utf-8')
      for rule in rules:
          if re.match(rule, url):
              return True
      return False

  def should_walk(self, url, urls, context):
    return True

  def walk_one(self, url, context):
    page = Fetcher(url)
    succ = page.fetch()
    if not succ:
      logging.error("Walk %s failed" % (url))
      return
    # Use url as default context
    if context == None:
      context = url
    urls = []
    for i, url in enumerate(page):
      url = url.encode('utf-8')
      in_white = self.match_rule_list(url, self.crawler.white_rules)
      in_black = self.match_rule_list(url, self.crawler.black_rules)
      # Custom filtering
      should_walk = self.should_walk(url, urls, context)
      if in_white and not in_black and should_walk:
        urls.append(url)
    return urls

  def walk(self, task):
    url = task[0]
    level = task[1]
    context = task[2]
    crawler = self.crawler
    urls = self.walk_one(url, context)
    if not urls:
      return False
    crawler.append(urls, level, context)
    return True

class Crawler(object):
  def __init__(self, white_rules, black_rules, max_level = 2, max_thread = 10, thread_klass = CrawlerThread):
    self.white_rules = white_rules
    self.black_rules = black_rules
    self.max_level = max_level
    self.urls = []
    self._walked = []
    self.max_thread = max_thread
    self._threads = []
    self.queue = Queue.Queue()
    self.shouldExit = False
    self._thread_klass = thread_klass
    self.fail_count = 0

  def append(self, urls, level, context = None):
    # Append results
    for url in urls:
      if url not in self.urls:
        self.urls.append(url)
    # TODO: custom append
    self.on_append(urls, level, context)
    # Append queue
    if level < self.max_level:
      next_level = level + 1
      for url in urls:
        if url not in self._walked:
          self.queue.put((url, next_level, context))
          self._walked.append(url)
    if level != 0:
      self.queue.task_done()
    pass

  def on_append(self, urls, level, context = None):
    pass

  def _init_threads(self):
    for id in range(self.max_thread):
      t = self._thread_klass(self, id)
      self._threads.append(t)
      t.start()

  def walk(self, urls):
    # Create threads to size
    self._init_threads()
    self.append(urls, 0)
    # Wait
    self.queue.join()
    # Notify threads to exit
    self.shouldExit = True
    # Wait
    for t in self._threads:
      t.join()

  def walk_with_context(self, tasks):
    # Tasks = [(url, context), ...]
    self._init_threads()
    for task in tasks:
      url = task[0]
      context = task[1]
      self.append([url], 0, context)
    # Wait
    self.queue.join()
    # Notify threads to exit
    self.shouldExit = True
    # Wait
    for t in self._threads:
      t.join()

def main():
  url = 'http://www.nmc.gov.cn/publish/radar/beijing.htm'
  white_rules = ['(http:\\/\\/www\\.nmc\\.gov\\.cn\\/publish\\/radar\\/)']
  black_rules = ['(http:\\/\\/www\\.nmc\\.gov\\.cn\\/publish\\/radar\\/)(chinaall|stationindex)\\.htm']
  #poolAllLinks([url], white_rules, black_rules)
  crawler = Crawler(white_rules, black_rules)
  crawler.walk([url])
  for url in crawler.urls:
    print url
  print "Found %d stations" % (len(crawler.urls))
  return
  img_rules = ["javascript:view_text_img\((\'.*?\'),(\'.*?\'),'','','','',(\'.*?\'),'',(\'.*?\'),'',''\)"]
  img_crawler = Crawler(img_rules, [], 1)
  img_crawler.walk(crawler.urls)
  for url in img_crawler.urls:
    print url
  print "Found %d frames" % (len(img_crawler.urls))
if __name__ == "__main__":
  main()
