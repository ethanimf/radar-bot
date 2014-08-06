import webapp2
import logging

CRON_HEADER = 'X-AppEngine-Cron'

class TaskHandler(webapp2.RequestHandler):
  def get(self):
    if self.request.headers.get(CRON_HEADER, False):
      logging.info("Start task: " + self.get_name())
      self.run_task()
    else:
      logging.error("Direct access to task: " + self.get_name())
      self.response.write("<img src='../fail.gif' />")
      self.response.write("<p>This is why we can't have nice things.</p>")
