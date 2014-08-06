import webapp2

class MainHandler(webapp2.RequestHandler):
  def get(self):
    self.response.write('What are you looking at? Get Lost!')
