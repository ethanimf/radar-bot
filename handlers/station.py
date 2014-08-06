from task_base import *

class StationTaskHandler(TaskHandler):
  def get_name(self):
    return 'station'

  def run_task(self):
    self.response.write("I'll get all stations")
