import sys
sys.path.insert(0, 'lib')
from lib.github import *
import urllib2
import config
import base64
import logging
import time
from datetime import datetime
import Queue
import threading
import json

class BuilderThread(threading.Thread):
  def __init__(self, builder, id, max_retry = 3):
    threading.Thread.__init__(self)
    self.builder = builder
    self.id = id
    self.max_retry = max_retry
    self.current_retry = 0
    self.init()

  def init(self):
    pass

  def run(self):
    builder = self.builder
    queue = builder.queue
    logging.debug("Thread %d started" % (self.id))
    while not builder.should_exit:
      if not queue.empty():
        task = queue.get()
        self.current_retry = 0
        succ = False
        while self.current_retry <= self.max_retry:
          if self.current_retry > 0:
            logging.warning("Failed last time, retrying (%d/%d)" % (self.current_retry, self.max_retry))
          # Build
          succ = self.build(task)
          if succ:
            break
          self.current_retry += 1
        if not succ:
          logging.error("Fail to run %s" % (task[0]))
          queue.task_done()
          builder.fail_count += 1
      time.sleep(1)
  def build(self, task):
    self.builder.append(task)
    return True

class Builder(object):
  def __init__(self, max_thread = 10, thread_klass = BuilderThread):
    self.max_thread = max_thread
    self._threads = []
    self.queue = Queue.Queue()
    self.should_exit = False
    self._thread_klass = thread_klass
    self.fail_count = 0
    self.results = []

  def append(self, result):
    self.results.append(result)
    if self.on_append(result):
      self.queue.task_done()

  def on_append(self, result):
    return True

  def _init_threads(self):
    for id in range(self.max_thread):
      t = self._thread_klass(self, id)
      self._threads.append(t)
      t.start()

  def build(self, tasks):
    # Create threads to size
    self._init_threads()
    logging.info("Put %d tasks" % (len(tasks)))
    # Put tasks
    for task in tasks:
      self.queue.put(task)
    logging.info("Waiting for tasks")
    # Wait
    self.queue.join()
    logging.info("Tasks completed")
    # Notify threads to exit
    self.should_exit = True
    # Wait
    for t in self._threads:
      t.join()
    logging.info("Threads completed")

class RepoBuilder(Builder):
  def __init__(self, repo, max_thread = 10, thread_klass = BuilderThread):
    Builder.__init__(self, max_thread, thread_klass)
    self.repo = repo

BASE_URL = "http://image.weather.gov.cn"

class BlobBuilderThread(BuilderThread):
  def download_to_base64(self, url):
    content = None
    try:
      remote_file = urllib2.urlopen(url ,timeout = 10)
      content = base64.encodestring(remote_file.read())
    except Exception as e:
      logging.error("Fail to download %s: %s" % (url, e))
    return content

  def create_blob(self, content):
    sha = None
    try:
      blob = self.builder.repo.create_git_blob(content, 'base64')
      sha = blob.sha
    except Exception as e:
      logging.error("Fail to create blob: %s" % (e))
    return sha

  def build(self, task):
    logging.info("Building blob from %s" % (task.url))
    content = self.download_to_base64(BASE_URL + task.url)
    if not content:
      return False
    sha = self.create_blob(content)
    if not sha:
      return False
    task.blob = sha
    logging.info("Blob: %s" % (sha))
    self.builder.append(task)
    return True

class TreeBuilderThread(BuilderThread):
  def build_frame_json(self, frame_names):
    # Newest first
    frame_names.sort(reverse = True)
    return json.dumps(frame_names)

  def build(self, task):
    id = task[0]
    frames = task[1]
    old_tree_sha = task[2]
    logging.info("Build tree %s for %d frames based upon %s" % (id, len(frames), old_tree_sha))
    # Get old tree
    old_tree = None
    if old_tree_sha:
      # Get old tree
      try:
        old_tree = self.builder.repo.get_git_tree(old_tree_sha)
      except Exception as e:
        logging.error("Fail to get tree %s: %s" % (old_tree_sha, e))
        return False
    # Create tree element list
    elements = [InputGitTreeElement(frame.get_file_name(), '100644', 'blob', sha = frame.blob) for frame in frames]
    if old_tree:
      elements += [InputGitTreeElement(e.path, e.mode, e.type, sha = e.sha) for e in old_tree.tree]
    # Build frames.json
    frame_names = []
    for el in elements:
      identity = el._identity
      if identity['type'] != 'blob' or identity['path'] == 'frames.json':
        continue
      frame_names.append(identity['path'])
    json_content = self.build_frame_json(frame_names)
    # Create frames.json blob
    json_blob = self.builder.repo.create_git_blob(json_content, 'utf-8')
    elements.append(InputGitTreeElement('frames.json', '100644', 'blob', sha = json_blob.sha))
    # Create tree
    tree = self.builder.repo.create_git_tree(elements)
    for frame in frames:
      frame.tree = tree.sha
    self.builder.append((id, tree.sha))
    logging.info("Tree: %s" % (tree.sha))
    return True

def print_tree(repo, sha, path = '/'):
  print "Tree %s:" % (path)
  sub_trees = []
  tree = repo.get_git_tree(sha)
  for e in tree.tree:
    print "%s %s %s" % (e.sha, e.type, e.path)
    if e.type == 'tree':
      sub_trees.append((e.sha, e.path))
  [print_tree(repo, s[0], path + s[1] + '/') for s in sub_trees]

class GitHubDeployer(object):
  def __init__(self, payload):
    self.payload = payload

  def auth(self):
    for account in config.ACCOUNTS:
      if self.auth_one(account[0], account[1]):
        return True
    logging.error("All GitHub accounts failed to be authenticated")

  def auth_one(self, username, password):
    logging.info("Log in %s" % (username))
    g = Github(username, password)
    self.g = g

    user = g.get_user()
    rate = g.rate_limiting
    logging.info("Logged in as %s, rate: (%d/%d)" % (user.login, rate[0], rate[1]))
    self.user = user

    if rate[0] < 1000:
      logging.warning("Not enough GitHub API Rate, try another")
      return False

    orgs = user.get_orgs()
    org = None
    for o in orgs:
      if o.login == config.ORG_NAME:
        org = o
    if org != None:
      logging.info("Find organization: %s" % org.login)
    else:
      logging.error("Cannot find organization: %s" % config.ORG_NAME)
      return False
    self.org = org

    repo = org.get_repo(config.REPO_NAME)
    if repo != None:
      logging.info("Find repository: %s" % repo.name)
    else:
      logging.error("Cannot find organization: %s" % config.REPO_NAME)
      return False
    self.repo = repo

    perm = repo.permissions
    logging.info("Premissions:")
    logging.info("- ADMIN : %s" % (perm.admin))
    logging.info("- PULL  : %s" % (perm.pull))
    logging.info("- PUSH  : %s" % (perm.push))

    if not perm.admin:
      logging.error("Insufficient permissions")
      return False

    branch = repo.get_branch(config.BRANCH)
    if not branch:
      logging.error("Unable to find branch: %s" % (config.BRANCH))
      return False
    logging.info("Find branch %s @%s" % (branch.name, branch.commit.sha))
    self.branch = branch
    self.ref = repo.get_git_ref("heads/%s" % (config.BRANCH))
    return True

  def deploy(self):
    if not self.auth():
      return False
    # Check
    if len(self.payload) == 0:
      logging.info("Nothing to deploy")
      return True
    # Prepare blobs for every frame
    all_frames = reduce(lambda f1, f2: f1 + f2, self.payload.values())
    blob_builder = RepoBuilder(self.repo, thread_klass = BlobBuilderThread)
    blob_builder.build(all_frames)
    # Read old trees
    last_commit_sha = self.branch.commit.sha
    logging.info("Reading root tree @%s" % (last_commit_sha))
    root = self.repo.get_git_tree(last_commit_sha)
    old_trees = {}
    for e in root.tree:
      if e.type != 'tree':
        continue
      if self.payload.has_key(e.path):
        old_trees[e.path] = e.sha
    #print_tree(self.repo, self.branch.commit.sha)
    # Prepare trees for every stations
    logging.info("Creating sub trees")
    all_trees = []
    for id, frames in self.payload.iteritems():
      all_trees.append((id, frames, old_trees.get(id)))
    tree_builder = RepoBuilder(self.repo, thread_klass = TreeBuilderThread)
    tree_builder.build(all_trees)
    # Make new root tree
    logging.info("Create new root")
    new_trees = {}
    for t in tree_builder.results:
      new_trees[t[0]] = t[1]
    new_root_elements = []
    for e in root.tree:
      if e.type == 'tree' and new_trees.has_key(e.path):
        new_sha = new_trees[e.path]
        logging.info("Replace tree %s with %s" % (e.path, new_sha))
        # replace with new tree
        new_e = InputGitTreeElement(e.path, '040000', 'tree', sha = new_sha)
        new_root_elements.append(new_e)
        del new_trees[e.path]
      else:
        new_root_elements.append(InputGitTreeElement(e.path, e.mode, e.type, sha = e.sha))

    for path in new_trees:
      new_sha = new_trees[path]
      logging.info("Add new tree %s to %s" % (new_sha, path))
      new_e = InputGitTreeElement(path, '040000', 'tree', sha = new_sha)
      new_root_elements.append(new_e)


    new_root = self.repo.create_git_tree(new_root_elements)
    logging.info("New root tree: %s" % (new_root.sha))
    # Make commit
    parent_commit = self.repo.get_git_commit(last_commit_sha)
    message = "Update %d frames for %d stations at %s" % (len(all_frames), len(self.payload), datetime.now())
    new_commit = self.repo.create_git_commit(message, new_root, [parent_commit])
    logging.info("Commit: %s" % new_commit.sha)
    logging.info("Message: %s" % message)
    # Update ref
    logging.info("Update ref")
    self.ref.edit(new_commit.sha)
    # Done
    logging.info("Deploy finished")
    return True

def open_and_encode(path):
  with open(path, 'rb') as image_f:
    return base64.b64encode(image_f.read())

def main():
  # print "Log in %s" % (config.GITHUB_ACCOUNT)
  # g = Github(config.GITHUB_ACCOUNT, config.GITHUB_PASSWORD)
  # user = g.get_user()
  # print "Logged in as %s" % (user.login)
  # print "List repositories:"
  # repos = user.get_repos()
  # for repo in repos:
  #   print repo.name
  # print "List organizations:"
  # orgs = user.get_orgs()
  # org = None
  # for o in orgs:
  #   print "- %s" % (o.login)
  #   if o.login == config.ORG_NAME:
  #     org = o
  # if org != None:
  #   print "Find organization: %s" % org.login
  # else:
  #   print "Cannot find organization: %s" % config.ORG_NAME
  #   return
  #
  # repo = org.get_repo(config.REPO_NAME)
  # if repo != None:
  #   print "Find repository: %s" % repo.name
  # else:
  #   print "Cannot find organization: %s" % config.REPO_NAME
  #   return
  #
  # perm = repo.permissions
  # print "Premissions:"
  # print "- ADMIN : %s" % (perm.admin)
  # print "- PULL  : %s" % (perm.pull)
  # print "- PUSH  : %s" % (perm.push)
  #
  # if not perm.admin:
  #   print "Insufficient permissions, exiting"
  #   return
  #
  # branch = repo.get_branch(config.BRANCH)
  # if not branch:
  #   print "Unable to find branch: %s" % (config.BRANCH)
  #   return
  # print "Find branch %s @%s" % (branch.name, branch.commit.sha)
  #
  # ref = repo.get_git_ref("heads/%s" % (config.BRANCH))
  # print_tree(repo, branch.commit.sha)
  # return
  # if not ref:
  #   print "Unable to find ref for that branch"
  #   return
  # print "Ref: %s" % (ref.ref)
  # f1 = 'SEVP_AOC_RDCP_SLDAS_EBREF_AZ9230_L88_PI_20140808145500000.gif'
  # f2 = 'SEVP_AOC_RDCP_SLDAS_EBREF_AZ9230_L88_PI_20140808150000000.gif'
  # # TODO: open/download frames to base64
  # print "Open and encoding"
  # f1_c1 = open_and_encode('test_data/frame1/9230/' + f1)
  # f1_c2 = open_and_encode('test_data/frame1/9230/' + f2)
  # # TODO: create git blobs
  # print "Creating blob"
  # f1_b1 = repo.create_git_blob(f1_c1, 'base64')
  # print f1_b1.sha
  # f1_b2 = repo.create_git_blob(f1_c2, 'base64')
  # print f1_b2.sha
  # print "Creating tree"
  # # TODO: get original station tree if exists
  # f1_tree_e1 = InputGitTreeElement(f1, '100644', 'blob', sha = f1_b1.sha)
  # f1_tree_e2 = InputGitTreeElement(f2, '100644', 'blob', sha = f1_b2.sha)
  # f1_tree_contents = [f1_tree_e1, f1_tree_e2]
  # # TODO: create git trees (clone original first, then add new frame blob)
  # f1_tree_9230 = repo.create_git_tree(f1_tree_contents)
  # print f1_tree_9230.sha
  # f1_tree_root = repo.create_git_tree([InputGitTreeElement('9230', '040000', 'tree', sha = f1_tree_9230.sha)])
  # print f1_tree_root.sha
  # # TODO: create commit
  # print "Creating commit"
  # print "Parent: %s" % (parent_commit.sha)
  # f1_commit = repo.create_git_commit("Update frame1", f1_tree_root, [parent_commit])
  # print f1_commit.sha
  # # TODO: update ref
  # print "Update ref"
  # ref.edit(f1_commit.sha)
  pass

if __name__ == '__main__':
  main()
