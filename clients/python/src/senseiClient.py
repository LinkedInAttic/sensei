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

PARAM_OFFSET = "start"
PARAM_COUNT = "rows"
PARAM_QUERY = "q"
PARAM_QUERY_PARAM = "qparam"
PARAM_SORT = "sort"
PARAM_SORT_ASC = "asc"
PARAM_SORT_DESC = "desc"
PARAM_SORT_SCORE = "relevance"
PARAM_SORT_SCORE_REVERSE = "relrev"
PARAM_SORT_DOC = "doc"
PARAM_SORT_DOC_REVERSE = "docrev"
PARAM_FETCH_STORED = "fetchstored"
PARAM_SHOW_EXPLAIN = "showexplain"
PARAM_ROUTE_PARAM = "routeparam"
PARAM_SELECT = "select"
PARAM_SELECT_VAL = "val"
PARAM_SELECT_NOT = "not"
PARAM_SELECT_OP = "op"
PARAM_SELECT_OP_AND = "and"
PARAM_SELECT_OP_OR = "or"
PARAM_SELECT_PROP = "prop"
PARAM_FACET = "facet"
PARAM_DYNAMIC_INIT = "dyn"
PARAM_PARTITIONS = "partitions"

PARAM_FACET_EXPAND = "expand"
PARAM_FACET_MAX = "max"
PARAM_FACET_MINHIT = "minhit"
PARAM_FACET_ORDER = "order"
PARAM_FACET_ORDER_HITS = "hits"
PARAM_FACET_ORDER_VAL = "val"

PARAM_DYNAMIC_TYPE = "type"
PARAM_DYNAMIC_TYPE_STRING = "string"
PARAM_DYNAMIC_TYPE_BYTEARRAY = "bytearray"
PARAM_DYNAMIC_TYPE_BOOL = "boolean"
PARAM_DYNAMIC_TYPE_INT = "int"
PARAM_DYNAMIC_TYPE_LONG = "long"
PARAM_DYNAMIC_TYPE_DOUBLE = "double"
PARAM_DYNAMIC_VAL = "vals"

PARAM_RESULT_PARSEDQUERY = "parsedquery"
PARAM_RESULT_HIT_STORED_FIELDS = "stored"
PARAM_RESULT_HIT_STORED_FIELDS_NAME = "name"
PARAM_RESULT_HIT_STORED_FIELDS_VALUE = "val"
PARAM_RESULT_HIT_EXPLANATION = "explanation"
PARAM_RESULT_FACETS = "facets"

PARAM_RESULT_TID = "tid"
PARAM_RESULT_TOTALDOCS = "totaldocs"
PARAM_RESULT_NUMHITS = "numhits"
PARAM_RESULT_HITS = "hits"
PARAM_RESULT_HIT_UID = "uid"
PARAM_RESULT_HIT_DOCID = "docid"
PARAM_RESULT_HIT_SCORE = "score"
PARAM_RESULT_HIT_SRC_DATA = "srcdata"
PARAM_RESULT_TIME = "time"

PARAM_SYSINFO_NUMDOCS = "numdocs"
PARAM_SYSINFO_LASTMODIFIED = "lastmodified"
PARAM_SYSINFO_VERSION = "version"
PARAM_SYSINFO_FACETS = "facets"
PARAM_SYSINFO_FACETS_NAME = "name"
PARAM_SYSINFO_FACETS_RUNTIME = "runtime"
PARAM_SYSINFO_FACETS_PROPS = "props"
PARAM_SYSINFO_CLUSTERINFO = "clusterinfo"
PARAM_SYSINFO_CLUSTERINFO_ID = "id"
PARAM_SYSINFO_CLUSTERINFO_PARTITIONS = "partitions"
PARAM_SYSINFO_CLUSTERINFO_NODELINK = "nodelink"
PARAM_SYSINFO_CLUSTERINFO_ADMINLINK = "adminlink"

PARAM_RESULT_HITS_EXPL_VALUE = "value"
PARAM_RESULT_HITS_EXPL_DESC = "description"
PARAM_RESULT_HITS_EXPL_DETAILS = "details"

PARAM_RESULT_FACET_INFO_VALUE = "value"
PARAM_RESULT_FACET_INFO_COUNT = "count"
PARAM_RESULT_FACET_INFO_SELECTED = "selected"


class SenseiFacet:
	expand = False
	minHits = 1
	maxCounts = 10
	orderBy = PARAM_RESULT_HITS
	
	def __init__(self,expand=False,minHits=1,maxCounts=10,orderBy=PARAM_RESULT_HITS):
		if expand:
			self.expand = True
		self.minHits = minHits
		self.maxCounts = maxCounts
		self.orderBy = orderBy

class SenseiSelection:
	field = None
	values = []
	excludes = []
	properties = {}
	operation = PARAM_SELECT_OP_OR
	
	def __init__(self,field,oper=PARAM_SELECT_OP_OR):
		self.field = field
		self.operation = oper
		
	def addSelection(self,value,isNot=False):
		if isNot:
			self.excludes.append(value)
		else:
			self.values.append(value)
	
	def removeSelection(self,value,isNot=False):
		if isNot:
			self.excludes.remove(value)
		else:
			self.values.remove(value)
	
	def addProperty(self,name,value):
		self.properties[name]=value
	
	def removeProperty(self,name):
		del properties[name]

	def getSelectNotParam(self):
		return "%s.%s.%s" % (PARAM_SELECT, self.field, PARAM_SELECT_NOT)

	def getSelectNotParamValues(self):
		return ",".join(self.excludes)

	def getSelectOpParam(self):
		return "%s.%s.%s" % (PARAM_SELECT, self.field, PARAM_SELECT_OP)

	def getSelectValParam(self):
		return "%s.%s.%s" % (PARAM_SELECT, self.field, PARAM_SELECT_VAL)

	def getSelectValParamValues(self):
		return ",".join(self.values)

	def getSelectPropParam(self):
		return "%s.%s.%s" % (PARAM_SELECT, self.field, PARAM_SELECT_PROP)

	def getSelectPropParamValues(self):
		return ",".join(key + ":" + self.properties.get(key)
				for key in self.properties.keys())
	
class SenseiSort:
	field = ""
	dir = ""
	def __init__(self,field,reverse=False):
		self.field = field
		if not (field == PARAM_SORT_SCORE or
			field == PARAM_SORT_SCORE_REVERSE or
			field == PARAM_SORT_DOC or
			field == PARAM_SORT_DOC_REVERSE):
			if reverse:
				self.dir = PARAM_SORT_DESC
			else:
				self.dir = PARAM_SORT_ASC

	def buildSortField(self):
		if self.dir == "":
			return self.field
		else:
			return self.field + ":" + self.dir

class SenseiRequest:
	facets = {}
	selections = []
	sorts = None
	query = None
	qParam = {}
	offset = 0
	count = 10
	explain = False
	fetch = False
	routeParam = None
	
class SenseiHit:
	docid = None
	uid = None
	srcData = None
	score = None
	fieldVals = None
	explanation = None
	stored = None
	
	def load(self,json):
		self.docid = json.get(PARAM_RESULT_HIT_DOCID)
		self.uid = json.get(PARAM_RESULT_HIT_UID)
		self.score = json.get(PARAM_RESULT_HIT_SCORE)
		self.srcData = json.get(PARAM_RESULT_HIT_SRC_DATA)
		self.fieldVals = json
		self.explanation = json.get(PARAM_RESULT_HIT_EXPLANATION)
		self.stored = json.get(PARAM_RESULT_HIT_STORED_FIELDS)
	
class SenseiResultFacet:
	value = None
	count = None
	selected = None
	
	def load(self,json):
		self.value=json.get(PARAM_RESULT_FACET_INFO_VALUE)
		self.count=json.get(PARAM_RESULT_FACET_INFO_COUNT)
		self.selected=json.get(PARAM_RESULT_FACET_INFO_SELECTED,False)
	
class SenseiResult:
	numHits = 0
	totalDocs = 0
	time = 0
	parsedQuery = None
	hits = None
	facetMap = None
	jsonMap = None
	
	def load(self,json):
		self.jsonMap = json
		self.parsedQuery = json.get(PARAM_RESULT_PARSEDQUERY)
		self.totalDocs = json.get(PARAM_RESULT_TOTALDOCS,0)
		self.time = json.get(PARAM_RESULT_TIME,0)
		self.numHits = json.get(PARAM_RESULT_NUMHITS,0)
		hitList = json.get(PARAM_RESULT_HITS)
		if hitList:
			hits = []
			for hit in hitList:
				senseiHit = SenseiHit()
				senseiHit.load(hit)
				hits.append(senseiHit)
		facetMap = json.get(PARAM_RESULT_FACETS)
		if facetMap:
			facetMap = {}
			for k,v in facetMap.items():
				facetObj = SenseiResultFacet()
				facetObj.load(v)
				facetMap[k]=facetObj
	
class SenseiClient:
	host = None
	port = None
	opener = None
	path = None
	url = None
	def __init__(self,host='localhost',port=8080,path='sensei'):
		self.host = host
		self.port = port
		self.path = path
		self.url = 'http://%s:%d/%s' % (self.host,self.port,self.path)
		self.opener = urllib2.build_opener()
		self.opener.addheaders = [('User-agent', 'Python-urllib/2.5')]
		self.opener.addheaders = [('User-agent', 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_6_7) AppleWebKit/534.30 (KHTML, like Gecko) Chrome/12.0.742.91 Safari/534.30')]
		
	@staticmethod
	def buildUrlString(req):
		paramMap = {}
		paramMap[PARAM_OFFSET] = req.offset
		paramMap[PARAM_COUNT] = req.count
		if req.query:
			paramMap[PARAM_QUERY]=req.query
		if req.explain:
			paramMap[PARAM_SHOW_EXPLAIN] = "true"
		if req.fetch:
			paramMap[PARAM_FETCH_STORED] = "true"
		if req.routeParam:
			paramMap[PARAM_ROUTE_PARAM] = req.routeParam

		# paramMap["offset"] = req.offset
		# paramMap["count"] = req.count

		if req.sorts:
			paramMap[PARAM_SORT] = ",".join(sort.buildSortField() for sort in req.sorts)

		if req.qParam.get("query"):
			paramMap[PARAM_QUERY] = req.qParam.get("query")
		paramMap["qparam"] = ",".join(param + ":" + req.qParam.get(param)
					      for param in req.qParam.keys() if param != "query")

		for selection in req.selections:
			paramMap[selection.getSelectNotParam()] = selection.getSelectNotParamValues()
			paramMap[selection.getSelectOpParam()] = selection.operation
			paramMap[selection.getSelectValParam()] = selection.getSelectValParamValues()
			if selection.properties:
				paramMap[selection.getSelectPropParam()] = selection.getSelectPropParamValues()

		for facetName, facetSpec in req.facets.iteritems():
			paramMap["%s.%s.%s" % (PARAM_FACET, facetName, PARAM_FACET_MAX)] = facetSpec.maxCounts
			paramMap["%s.%s.%s" % (PARAM_FACET, facetName, PARAM_FACET_ORDER)] = facetSpec.orderBy
			paramMap["%s.%s.%s" % (PARAM_FACET, facetName, PARAM_FACET_EXPAND)] = facetSpec.expand
			paramMap["%s.%s.%s" % (PARAM_FACET, facetName, PARAM_FACET_MINHIT)] = facetSpec.minHits

		return urllib.urlencode(paramMap)
		
	def doQuery(self,req=None):
		paramString = None
		if req:
			paramString = SenseiClient.buildUrlString(req)
		urlReq = urllib2.Request(self.url,paramString)
		res = self.opener.open(urlReq)
		line = res.read()
		jsonObj = dict(json.loads(line))
		res = SenseiResult()
		res.load(jsonObj)
		return res

def testSort1():
	print "==== Testing sort1 ====" 
	req = SenseiRequest()
	req.offset = 0
	req.count = 4

	sort1 = SenseiSort("relevance")
	req.sorts = [sort1]
	
	client = SenseiClient()
	client.doQuery(req)

# XXX Does NOT work yet
def testSort2():
	print "==== Testing sort2 ====" 
	req = SenseiRequest()
	req.offset = 0
	req.count = 4

	sort1 = SenseiSort("year", True)
	sort2 = SenseiSort("relevance")
	req.sorts = [sort1, sort2]
	
	client = SenseiClient()
	client.doQuery(req)


def testQueryParam():
	print "==== Testing query params ====" 
	req = SenseiRequest()
	req.offset = 0
	req.count = 4

	sort1 = SenseiSort("relevance")
	req.sorts = [sort1]

	qParam = {}
	qParam["query"] = "cool car"
	qParam["param1"] = "value1"
	qParam["param2"] = "value2"
	req.qParam = qParam
	
	client = SenseiClient()
	client.doQuery(req)

def testSelection():
	print "==== Testing selections ====" 
	req = SenseiRequest()
	req.offset = 0
	req.count = 4

	select1 = SenseiSelection("color", "or")
	select1.addSelection("red")
	select1.addSelection("yellow")
	select1.addSelection("black", True)
	select1.addProperty("aaa", "111")
	select1.addProperty("bbb", "222")

	select2 = SenseiSelection("price")
	select2.addSelection("[* TO 6700]")
	select2.addSelection("[10000 TO 13100]")
	select2.addSelection("[13200 TO 17300]")

	req.selections = [select1]
	client = SenseiClient()
	client.doQuery(req)

def testFacetSpecs():
	print "==== Testing facet specs ====" 
	req = SenseiRequest()
	req.offset = 0
	req.count = 4

	facet1 = SenseiFacet()
	facet2 = SenseiFacet(True, 1, 3, PARAM_FACET_ORDER_VAL)
	facet3 = SenseiFacet(True, 1, 3, PARAM_FACET_ORDER_VAL)
	
	client = SenseiClient()
	res = client.doQuery(req)
	print res.jsonMap

if __name__ == "__main__":

	# Testing...

	testSort1()
	testQueryParam()
	testSelection()
	testFacetSpecs()

	#
	# XXX: Initializing runtime facet parameters
	#

	#
	# XXX: Partition Params
	#

