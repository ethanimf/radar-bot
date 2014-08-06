from task_base import *

class FrameTaskHandler(TaskHandler):
  def get_name(self):
    return 'frame'

  def run_task(self):
    self.response.write("I'll get all frames")
