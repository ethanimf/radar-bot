import re
import sys
import urllib2
import urlparse
from cgi import escape
sys.path.insert(0, 'lib')
from threadpool import *
from bs4 import BeautifulSoup
import logging

__all__ = ['Fetcher', 'Crawler']

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
    request, handle = self.open()
    self._addHeaders(request)
    if handle:
      try:
        content = unicode(handle.open(request, timeout=10).read(), "utf-8",
          errors="replace")
        soup = BeautifulSoup(content)
        tags = soup('a')
      except urllib2.HTTPError, error:
        if error.code == 404:
          logging.error("ERROR: %s -> %s" % (error, error.url))
        else:
          logging.error("ERROR: %s" % error)
        tags = []
      except urllib2.URLError, error:
        logging.error("ERROR: %s" % error)
        tags = []
      for tag in tags:
        href = tag.get("href")
        if href is not None:
          url = urlparse.urljoin(self.url, escape(href))
          if url not in self:
            self.urls.append(url)

class Crawler(object):
  def __init__(self, white_rules, black_rules, max_level = 2):
    self.white_rules = white_rules
    self.black_rules = black_rules
    self.max_level = max_level
    self.urls = []
    self._walked = []

  def match_rule_list(self, url, rules):
      #url = url.encode('utf-8')
      for rule in rules:
          if re.match(rule, url):
              return True
      return False

  def walk_one(self, url):
    page = Fetcher(url)
    page.fetch()
    urls = []
    for i, url in enumerate(page):
      url = url.encode('utf-8')
      in_white = self.match_rule_list(url, self.white_rules)
      in_black = self.match_rule_list(url, self.black_rules)
      if in_white and not in_black:
        urls.append(url)
    return urls

  def add_to_pool(self, urls, level):
    tasks = [({'self': self, 'url': url, 'level': level}) for url in urls]
    requests = makeRequests(self.worker, tasks)
    [self.pool.putRequest(req) for req in requests]

  def walk(self, urls):
    self.pool = ThreadPool(10)
    self.add_to_pool(urls, 1)
    self.pool.wait()

  def worker(self, p):
    logging.debug("%d: %s" % (p['level'], p['url']))
    urls = self.walk_one(p['url'])
    for url in urls:
      if url not in self.urls:
        self.urls.append(url)
    logging.debug("Total: %d" % len(self.urls))
    if p['level'] < self.max_level:
      u = []
      for url in urls:
        if url not in self._walked:
          u.append(url)
          self._walked.append(url)
      self.add_to_pool(u, p['level'] + 1)

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
if __name__ == "__main__":
  main()
