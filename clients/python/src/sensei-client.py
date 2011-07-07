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

class SenseiFacetOrder:
	Hits = "hits"
	Value = "val"

class SenseiFacet:
	expand = False
	minHits = 1
	maxCounts = 10
	orderBy = SenseiFacetOrder.Hits
	
	def __init__(self,expand=False,minHits=1,maxCounts=10,orderBy=SenseiFacetOrder.Hits):
		self.expand = expand
		self.minHits = minHits
		self.maxCounts = maxCounts
		self.orderBy = orderBy
		

class SenseiPropery:
	name = None
	value = None
	def __init__(self,name,value):
		self.name = name
		self.value = value
		
class SenseiSelectionOperation:
	Or = "or"
	And = "and"

class SenseiSelection:
	field = None
	values = []
	excludes = []
	properties = {}
	operation = SenseiSelectionOperation.Or
	
	def __init__(self,field,oper=SenseiSelectionOperation.Or):
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
		properties[name]=value
	
	def removeProperty(self,name):
		del properties[name]
	
class SenseiSort:
	field = ""
	dir = ""
	def __init__(self,field,reverse=False):
		self.field = field
		if reverse:
			self.dir = "desc"
		else:
			self.dir = "asc"

class SenseiRequest:
	facets = None
	selections = None
	sorts = None
	query = None
	qParam = None
	offset = 0
	count = 10
	explain = False
	fetch = False
	routeParam = None
	
class ScoreExplanation:
	description = None
	value = None
	inner = None
	def __init__(self,description,value):
		self.description = description
		self.value = value

class SenseiHit:
	docid = None
	uid = None
	srcData = None
	score = None
	fieldVals = {}
	explaination = None
	
class SenseiResult:
	numHits = 0
	totalDocs = 0
	time = 0
	hits = []
	facetMap = {}
	
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
		paramMap["start"]=req.offset
		paramMap["rows"]=req.count
		if req.query:
			paramMap["q"]=req.query
		if req.explain:
			paramMap["showexplain"]="true"
		if req.fetch:
			paramMap["fetchstored"]="true"
		if req.routeParam:
			paramMap["routeparam"]=req.routeParam
		
		return urllib.urlencode(paramMap)
		
	def doQuery(self,req):
		paramString = SenseiClient.buildUrlString(req)
		urlReq = urllib2.Request(self.url,paramString)
		res = self.opener.open(urlReq)
		resObj = eval(res.read())
		print resObj
		
s = SenseiRequest()
c = SenseiClient()
c.doQuery(s)
