#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

"""Python client library for Sensei
"""

import urllib
import urllib2
import json
import sys
import logging
import datetime
from datetime import datetime
import time
import re

from bql_parser import BQLParser, BQLRequest
from sensei_components import *
from pyparsing import ParseException, ParseFatalException, ParseSyntaxException


logger = logging.getLogger("sensei_client")

# TODO:
#
# 1. Term vector
# 2. Section

def test(str):
  return

# def test(str):
#   try:
#     tokens = BQLstmt.parseString(str)
#     print "tokens =",        tokens
#     print "tokens.columns =", tokens.columns
#     print "tokens.index =",  tokens.index
#     print "tokens.where =", tokens.where
#     if tokens.where:
#       pass
#       # print "tokens.where.predicates =", tokens.where.predicates
#       # print "tokens.where.cumulative_preds =", tokens.where.cumulative_preds
#       # for predicate in tokens.where.predicates:
#       #   print "--------------------------------------"
#       #   print "predicate.value_list =", predicate.value_list
#       #   print "predicate.except_values =", predicate.except_values
#       #   print "predicate.prop_list =", predicate.prop_list
#       #   if predicate.cumulative_preds:
#       #     print "predicate.cumulative_preds =", predicate.cumulative_preds
#     print "tokens.orderby =", tokens.orderby
#     if tokens.orderby:
#       print "tokens.orderby.orderby_spec =", tokens.orderby.orderby_spec
#     print "tokens.limit =", tokens.limit
#     print "tokens.facet_specs =", tokens.facet_specs
#     print "tokens.groupby =", tokens.groupby
#     print "tokens.max_per_group =", tokens.max_per_group
#     print "tokens.given =", tokens.given
#     if tokens.given:
#       print "tokens.given.facet_param =", tokens.given.facet_param
#     print "tokens.fetching_stored =", tokens.fetching_stored
#   except ParseException as err:
#     # print " " * (err.loc + 2) + "^\n" + err.msg
#     pass
#   except ParseSyntaxException as err:
#     # print " " * (err.loc + 2) + "^\n" + err.msg
#     pass
#   except ParseFatalException as err:
#     # print " " * (err.loc + 2) + "^\n" + err.msg
#     pass
#   finally:
#     reset_all()


class SenseiClient:
  """Sensei client class."""

  def __init__(self, host='localhost', port=8080, path='sensei', sysinfo=None):
    self.host = host
    self.port = port
    self.path = path
    self.url = 'http://%s:%d/%s' % (self.host, self.port, self.path)
    self.opener = urllib2.build_opener()
    self.opener.addheaders = [('User-agent', 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_6_7) AppleWebKit/534.30 (KHTML, like Gecko) Chrome/12.0.742.91 Safari/534.30')]

    if sysinfo:
      self.sysinfo = SenseiSystemInfo(sysinfo)
    else:
      urlReq = urllib2.Request(self.url + "/sysinfo")
      res = self.opener.open(urlReq)
      line = res.read()
      jsonObj = json.loads(line)
      # print json.dumps(jsonObj, indent=4)
      self.sysinfo = SenseiSystemInfo(jsonObj)
    self.facet_map = {}
    for facet_info in self.sysinfo.get_facet_infos():
      self.facet_map[facet_info.get_name()] = facet_info

    self.parser = BQLParser(self.facet_map)

  def compile(self, bql_stmt):
    tokens = self.parser.parse(bql_stmt)
    if tokens:
      logger.debug("tokens: %s" % tokens)
      bql_req = BQLRequest(tokens, self.facet_map)
      return SenseiRequest(bql_req, facet_map=self.facet_map)
    return None

  def buildJsonString(self, req, sort_keys=True, indent=None):
    """Build a Sensei request in JSON format.

    Once built, a Sensei request in JSON format can be sent to a Sensei
    broker using the following command:

    $ curl -XPOST http://localhost:8080/sensei -d '{
      "fetchStored": "true", 
      "from": 0, 
      "size": 10
    }'

    """

    output_json = {}

    output_json[JSON_PARAM_FROM] = req.offset
    output_json[JSON_PARAM_SIZE] = req.count

    if req.query:
      output_json[JSON_PARAM_QUERY] = {
        JSON_PARAM_QUERY_STRING: {
          JSON_PARAM_QUERY: req.query
          }
        }

    if req.explain:
      output_json[JSON_PARAM_QUERY] = True
    if req.fetch_stored:
      output_json[JSON_PARAM_FETCH_STORED] = True
    if req.route_param:
      output_json[JSON_PARAM_ROUTEPARAM] = req.route_param
    if req.sorts:
      output_json[JSON_PARAM_SORT] = [sort.build_sort_spec() for sort in req.sorts]

    if req.filter:
      output_json[JSON_PARAM_FILTER] = req.filter

    if req.query_pred:
      output_json[JSON_PARAM_QUERY] = req.query_pred[JSON_PARAM_QUERY]

    if req.selections:
      output_json[JSON_PARAM_SELECTIONS] = req.selections

    facet_spec_map = {}
    for facet_name, facet_spec in req.facets.iteritems():
      facet_spec_map[facet_name] = {
        PARAM_FACET_MAX: facet_spec.maxCounts,
        PARAM_FACET_ORDER: facet_spec.orderBy,
        PARAM_FACET_EXPAND: facet_spec.expand,
        PARAM_FACET_MINHIT: facet_spec.minHits
        }
    if facet_spec_map:
      output_json[JSON_PARAM_FACETS] = facet_spec_map
      
    facet_init_map = {}
    for facet_name, initParams in req.facet_init_param_map.iteritems():
      inner_map = {}
      for name, vals in initParams.bool_map.iteritems():
        inner_map[name] = {PARAM_DYNAMIC_TYPE : PARAM_DYNAMIC_TYPE_BOOL,
                           "values" : vals}
      for name, vals in initParams.int_map.iteritems():
        inner_map[name] = {PARAM_DYNAMIC_TYPE : PARAM_DYNAMIC_TYPE_INT,
                           "values" : [safe_str(val) for val in vals]}
      for name, vals in initParams.long_map.iteritems():
        inner_map[name] = {PARAM_DYNAMIC_TYPE : PARAM_DYNAMIC_TYPE_LONG,
                           "values" : [safe_str(val) for val in vals]}
      for name, vals in initParams.string_map.iteritems():
        inner_map[name] = {PARAM_DYNAMIC_TYPE : PARAM_DYNAMIC_TYPE_STRING,
                           "values" : vals}
      for name, vals in initParams.byte_map.iteritems():
        inner_map[name] = {PARAM_DYNAMIC_TYPE : PARAM_DYNAMIC_TYPE_BYTEARRAY,
                           "values" : [safe_str(val) for val in vals]}
      for name, vals in initParams.double_map.iteritems():
        inner_map[name] = {PARAM_DYNAMIC_TYPE : PARAM_DYNAMIC_TYPE_DOUBLE,
                           "values" : [safe_str(val) for val in vals]}
      facet_init_map[facet_name] = inner_map
    if facet_init_map:
      output_json[JSON_PARAM_FACET_INIT] = facet_init_map

    if req.groupby:
      # For now we only support group-by on single column
      output_json[JSON_PARAM_GROUPBY] = {
        JSON_PARAM_COLUMNS: [req.groupby],
        JSON_PARAM_TOP: req.max_per_group
        }

    # print ">>> output_json = ", output_json
    return json.dumps(output_json, sort_keys=sort_keys, indent=indent)

  @staticmethod
  def buildUrlString(req):
    paramMap = {}
    paramMap[PARAM_OFFSET] = req.offset
    paramMap[PARAM_COUNT] = req.count
    if req.query:
      paramMap[PARAM_QUERY]=req.query
    if req.explain:
      paramMap[PARAM_SHOW_EXPLAIN] = "true"
    if req.fetch_stored:
      paramMap[PARAM_FETCH_STORED] = "true"
    if req.route_param:
      paramMap[PARAM_ROUTE_PARAM] = req.route_param

    if req.sorts:
      paramMap[PARAM_SORT] = ",".join(sort.build_sort_field() for sort in req.sorts)

    if req.qParam.get("query"):
      paramMap[PARAM_QUERY] = req.qParam.get("query")
      del req.qParam["query"]
    if req.qParam:
      paramMap[PARAM_QUERY_PARAM] = ",".join(param + ":" + req.qParam.get(param)
                                             for param in req.qParam.keys() if param != "query")

    for selection in req.selections.values():
      paramMap[selection.getSelectNotParam()] = selection.getSelectNotParamValues()
      paramMap[selection.getSelectOpParam()] = selection.operation
      paramMap[selection.getSelectValParam()] = selection.getSelectValParamValues()
      if selection.properties:
        paramMap[selection.getSelectPropParam()] = selection.getSelectPropParamValues()

    for facet_name, facet_spec in req.facets.iteritems():
      paramMap["%s.%s.%s" % (PARAM_FACET, facet_name, PARAM_FACET_MAX)] = facet_spec.maxCounts
      paramMap["%s.%s.%s" % (PARAM_FACET, facet_name, PARAM_FACET_ORDER)] = facet_spec.orderBy
      paramMap["%s.%s.%s" % (PARAM_FACET, facet_name, PARAM_FACET_EXPAND)] = facet_spec.expand and "true" or "false"
      paramMap["%s.%s.%s" % (PARAM_FACET, facet_name, PARAM_FACET_MINHIT)] = facet_spec.minHits

    for facet_name, initParams in req.facet_init_param_map.iteritems():
      for name, vals in initParams.bool_map.iteritems():
        paramMap["%s.%s.%s.%s" %
                 (PARAM_DYNAMIC_INIT, facet_name, name, PARAM_DYNAMIC_TYPE)] = PARAM_DYNAMIC_TYPE_BOOL
        paramMap["%s.%s.%s.%s" %
                 (PARAM_DYNAMIC_INIT, facet_name, name,
                  PARAM_DYNAMIC_VAL)] = ','.join([val and "true" or "false" for val in vals])
      for name, vals in initParams.int_map.iteritems():
        paramMap["%s.%s.%s.%s" %
                 (PARAM_DYNAMIC_INIT, facet_name, name, PARAM_DYNAMIC_TYPE)] = PARAM_DYNAMIC_TYPE_INT
        paramMap["%s.%s.%s.%s" %
                 (PARAM_DYNAMIC_INIT, facet_name, name,
                  PARAM_DYNAMIC_VAL)] = ','.join([safe_str(val) for val in vals])
      for name, vals in initParams.long_map.iteritems():
        paramMap["%s.%s.%s.%s" %
                 (PARAM_DYNAMIC_INIT, facet_name, name, PARAM_DYNAMIC_TYPE)] = PARAM_DYNAMIC_TYPE_LONG
        paramMap["%s.%s.%s.%s" %
                 (PARAM_DYNAMIC_INIT, facet_name, name,
                  PARAM_DYNAMIC_VAL)] = ','.join([safe_str(val) for val in vals])
      for name, vals in initParams.string_map.iteritems():
        paramMap["%s.%s.%s.%s" %
                 (PARAM_DYNAMIC_INIT, facet_name, name, PARAM_DYNAMIC_TYPE)] = PARAM_DYNAMIC_TYPE_STRING
        paramMap["%s.%s.%s.%s" %
                 (PARAM_DYNAMIC_INIT, facet_name, name,
                  PARAM_DYNAMIC_VAL)] = ','.join(vals)
      for name, vals in initParams.byte_map.iteritems():
        paramMap["%s.%s.%s.%s" %
                 (PARAM_DYNAMIC_INIT, facet_name, name, PARAM_DYNAMIC_TYPE)] = PARAM_DYNAMIC_TYPE_BYTEARRAY
        paramMap["%s.%s.%s.%s" %
                 (PARAM_DYNAMIC_INIT, facet_name, name,
                  PARAM_DYNAMIC_VAL)] = ','.join([safe_str(val) for val in vals])
      for name, vals in initParams.double_map.iteritems():
        paramMap["%s.%s.%s.%s" %
                 (PARAM_DYNAMIC_INIT, facet_name, name, PARAM_DYNAMIC_TYPE)] = PARAM_DYNAMIC_TYPE_DOUBLE
        paramMap["%s.%s.%s.%s" %
                 (PARAM_DYNAMIC_INIT, facet_name, name,
                  PARAM_DYNAMIC_VAL)] = ','.join([safe_str(val) for val in vals])

    if req.groupby:
      paramMap[PARAM_GROUP_BY] = req.groupby
      if req.max_per_group > 0:
        paramMap[PARAM_MAX_PER_GROUP] = req.max_per_group

    return urllib.urlencode(paramMap)
    
  def doQuery(self, req, using_json=True):
    """Execute a search query."""

    time1 = datetime.now()
    query_string = None
    if using_json: # Use JSON format
      query_string = self.buildJsonString(req)
    else:
      query_string = SenseiClient.buildUrlString(req)
    logger.debug(query_string)
    urlReq = urllib2.Request(self.url, query_string)
    res = self.opener.open(urlReq)
    line = res.read()
    jsonObj = json.loads(line)
    res = SenseiResult(jsonObj)
    delta = datetime.now() - time1
    res.total_time = delta.seconds * 1000 + delta.microseconds / 1000
    return res

  def get_sysinfo(self):
    return self.sysinfo

  def get_facet_map(self):
    return self.facet_map
  
  def run_example(self):
    """ a sample sensei request"""
    req = SenseiRequest();
    
    # sort;
    sort = SenseiSort("color", True)
    req.append_sort(sort)
    
    res = self.doQuery(req)
    res.display(columns=req.get_columns(), max_col_width=40)

def main(argv):
  print "Welcome to Sensei Shell"
  from optparse import OptionParser
  usage = "usage: %prog [options]"
  parser = OptionParser(usage=usage)
  parser.add_option("-w", "--column-width", dest="max_col_width",
                    default=100, help="Set the max column width")
  parser.add_option("-v", "--verbose", action="store_true", dest="verbose",
                    default=False, help="Turn on verbose mode")
  (options, args) = parser.parse_args()

  if options.verbose:
    logger.setLevel(logging.DEBUG)
  else:
    logger.setLevel(logging.INFO)

  formatter = logging.Formatter("%(asctime)s %(filename)s:%(lineno)d - %(message)s")
  stream_handler = logging.StreamHandler()
  stream_handler.setFormatter(formatter)
  logger.addHandler(stream_handler)

  if len(args) <= 1:
    client = SenseiClient()
    print "using default host=localhost, port=8080"
  else:
    host = args[0]
    port = int(args[1])
    print "Url specified, host: %s, port: %d" % (host,port)
    client = SenseiClient(host, port, 'sensei')

  import readline
  readline.parse_and_bind("tab: complete")
  while 1:
    try:
      stmt = raw_input('> ')
      if stmt == "exit":
        break
      # if options.verbose:
      #   test(stmt)
      if stmt == "sample request":
        client.run_example()
        
      req = client.compile(stmt)
      if req.stmt_type == "select":
        res = client.doQuery(req)
        res.display(columns=req.get_columns(), max_col_width=int(options.max_col_width))
      elif req.stmt_type == "desc":
        sysinfo = client.get_sysinfo()
        sysinfo.display()
    except EOFError:
      break
    except ParseException as err:
      print " " * (err.loc + 2) + "^\n" + err.msg
    except ParseSyntaxException as err:
      print " " * (err.loc + 2) + "^\n" + err.msg
    except ParseFatalException as err:
      print " " * (err.loc + 2) + "^\n" + err.msg
    except SenseiClientError as err:
      print err

if __name__ == "__main__":
  main(sys.argv)
