#!/usr/bin/env python
#
# Copyright 2007 Google Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
import webapp2
import logging

CRON_HEADER = 'X-AppEngine-Cron'

class MainHandler(webapp2.RequestHandler):
  def get(self):
    self.response.write('What are you looking at? Get Lost!')

class TaskHandler(webapp2.RequestHandler):
  def get(self):
    if self.request.headers.get(CRON_HEADER, False):
      logging.info("Start task: " + self.get_name())
      self.run_task()
    else:
      logging.error("Direct access to task: " + self.get_name())
      self.response.write("<img src='../fail.gif' />")
      self.response.write("<p>This is why we can't have nice things.</p>")

class StationTaskHandler(TaskHandler):
  def get_name(self):
    return 'station'

  def run_task(self):
    self.response.write("I'll get all stations")

class FrameTaskHandler(TaskHandler):
  def get_name(self):
    return 'frame'

  def run_task(self):
    self.response.write("I'll get all frames")

app = webapp2.WSGIApplication([
  ('/', MainHandler),
  ('/tasks/station', StationTaskHandler),
  ('/tasks/frame', FrameTaskHandler)
], debug=True)
