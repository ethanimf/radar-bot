import sys
#sys.path.insert(0, 'lib')
from github import *
import config

class GitHubDeployer(object):
  pass

def main():
  print "Log in %s" % (config.GITHUB_ACCOUNT)
  g = Github(config.GITHUB_ACCOUNT, config.GITHUB_PASSWORD)
  user = g.get_user()
  print "User: %s" % (user.name)
  pass

if __name__ == '__main__':
  main()
