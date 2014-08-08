import sys
sys.path.insert(0, 'lib')
from lib.github import *
import config
import base64
import logging

class GitHubDeployer(object):
  def __init__(self, payload):
    self.payload = payload

  def auth(self):
    logging.info("Log in %s" % (config.GITHUB_ACCOUNT))
    g = Github(config.GITHUB_ACCOUNT, config.GITHUB_PASSWORD)
    self.g = g

    user = g.get_user()
    logging.info("Logged in as %s" % (user.login))
    self.user = user

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

    return True

def open_and_encode(path):
  with open(path, 'rb') as image_f:
    return base64.b64encode(image_f.read())

def print_tree(repo, sha, path = '/'):
  print "Tree %s:" % (path)
  sub_trees = []
  tree = repo.get_git_tree(sha)
  for e in tree.tree:
    print "%s %s %s" % (e.sha, e.type, e.path)
    if e.type == 'tree':
      sub_trees.append((e.sha, e.path))
  [print_tree(repo, s[0], path + s[1] + '/') for s in sub_trees]

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
