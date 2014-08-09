from task_base import *
from deployers import *
import logging

class CleanTaskHandler(TaskHandler):
  def get_name(self):
    return 'clean'

  def run_task(self):
    max_retry = 5
    current_retry = 0
    succ = False
    while current_retry <= max_retry and not succ:
      if current_retry > 0:
        logging.warning("Retry (%d / %d)" % (current_retry, max_retry))
      deployer = GitHubDeployer(None)
      succ = deployer.clean()
      current_retry += 1

    if not succ:
      logging.error("Fail to clean")
      self.response.set_status(500)
    else:
      logging.info("Clean complete")
      self.response.set_status(200)
