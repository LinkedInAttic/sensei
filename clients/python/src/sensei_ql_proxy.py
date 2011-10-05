import sys, json
import random, os, subprocess
from twisted.internet import reactor
from twisted.web import server, resource
from twisted.web.static import File
from twisted.python import log
from datetime import datetime
import urllib, urllib2
import logging
import re

from sensei_client import *

PARSER_AGENT_PORT = 18888

client = SenseiClient("localhost",8080,'sensei')

#
# Main server resource
#
class Root(resource.Resource):
  
  def render_GET(self, request):
    """
    get response method for the root resource
    localhost:/18888
    """
    return 'Welcome to the REST API'

  def getChild(self, name, request):
    """
    We overrite the get child function so that we can handle invalid
    requests
    """
    print "root getchild"
    request.setHeader("Access-Control-Allow-Origin", "*")
    request.setHeader("Access-Control-Allow-Methods", "GET, POST")
    request.setHeader("Access-Control-Allow-Headers", "Origin, X-Requested-With, Accept") 
    
    if name == '':
      return self
    else:
      if name in VIEWS.keys():
        return VIEWS.get(name)#resource.Resource.getChild(self, name, request)
      else:
        return PageNotFoundError()

class PageNotFoundError(resource.Resource):

  def render_GET(self, request):
    return 'Page Not Found!'


class ParseBQL(resource.Resource):
  
  def getChild(self, name, request):
    """
    We overrite the get child function so that we can handle invalid
    requests
    """
    print "root getchild"
    request.setHeader("Access-Control-Allow-Origin", "*")
    request.setHeader("Access-Control-Allow-Methods", "GET, POST")
    request.setHeader("Access-Control-Allow-Headers", "Origin, X-Requested-With, Accept") 
  
  def render_OPTIONS(self,request):
  #  request.setHeader("Access-Control-Allow-Origin", "*")
  #  request.setHeader("Access-Control-Allow-Methods", "GET, POST")
  #  request.setHeader("Access-Control-Allow-Headers", "Origin, X-Requested-With, Accept")
    print "parse render options"
    return "ok"

  def render_GET(self, request):
    """Start a Sensei store."""
    
    try:
      info = request.args["info"][0]
      info = json.loads(info.encode('utf-8'))
      print ">>> info = ", info

      variables = re.findall(r"\$[a-zA-Z0-9]+", info["bql"])
      variables = list(set(variables))
      info["auxParams"] = [ {"name": var[1:]} for var in variables ]

      stmt = info["bql"]
      req = SenseiRequest(stmt)
      res = client.doQuery(req)
      print "numhits: %d" % res.numHits
      result = json.dumps(res.jsonMap)
      
      print result

      return json.dumps(
        {
          "ok": True,
          "result": res.jsonMap
          })
    except ParseException as err:
      print err
      return json.dumps(
        {
          "ok": False,
          "error": "Parsing error at location %s: %s" % (err.loc, err.msg)
          })

    except Exception as err:
      print err
      return "Error"

  def render_POST(self, request):
    return self.render_GET(request)

#to make the process of adding new views less static
VIEWS = {
  "parse": ParseBQL()
}

if __name__ == '__main__':
  params = {}
  # params["info"] = """{"name": "nus_member", "description": "xxx xxxx", "urn": "urn:feed:nus:member:exp:a:$memberId", 'bql': 'select * from cars where memberId in ("$memberId")'}"""
  params["info"] = """{"name": "nus_member", "description": "xxx xxxx"}"""
  print urllib.urlencode(params)

  root = Root()
  
  #for viewName, className in VIEWS.items():
    #add the view to the web service
   # root.putChild(viewName, className)
  log.startLogging(sys.stdout)
  log.msg('Starting parser agent: %s' %str(datetime.now()))
  server = server.Site(root)
  reactor.listenTCP(PARSER_AGENT_PORT, server)
  reactor.run()
