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

import urllib
import urllib2
import json
import sys
import logging
import datetime
from datetime import datetime
import time
import re

logger = logging.getLogger("sensei_components")

# Regular expression that matches a range facet value
RANGE_REGEX = re.compile(r'''\[(\d+(\.\d+)*|\*) TO (\d+(\.\d+)*|\*)\]''')

SELECTION_TYPE_RANGE = 1
SELECTION_TYPE_SIMPLE = 2
SELECTION_TYPE_TIME = 3

#
# REST API parameter constants
#
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
PARAM_GROUP_BY = "groupby"
PARAM_MAX_PER_GROUP = "maxpergroup"
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
PARAM_RESULT_SELECT_LIST = "select_list"

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

#
# JSON API parameter constants
#

JSON_PARAM_COLUMNS = "columns"
JSON_PARAM_EXPLAIN = "explain"
JSON_PARAM_FACETS = "facets"
JSON_PARAM_FACET_INIT = "facetInit"
JSON_PARAM_FETCH_STORED = "fetchStored"
JSON_PARAM_FETCH_TERM_VECTORS = "fetchTermVectors"
JSON_PARAM_FILTER = "filter"
JSON_PARAM_FROM = "from"
JSON_PARAM_GROUPBY = "groupBy"
JSON_PARAM_PARTITIONS = "partitions"
JSON_PARAM_QUERY = "query"
JSON_PARAM_QUERY_STRING = "query_string"
JSON_PARAM_ROUTEPARAM = "routeParam"
JSON_PARAM_SELECTIONS = "selections"
JSON_PARAM_SIZE = "size"
JSON_PARAM_SORT = "sort"
JSON_PARAM_TOP = "top"
JSON_PARAM_VALUES = "values"
JSON_PARAM_EXCLUDES = "excludes"
JSON_PARAM_OPERATOR = "operator"
JSON_PARAM_NO_OPTIMIZE = "_noOptimize"

# Group by related column names
GROUP_VALUE = "groupvalue"
GROUP_HITS = "grouphits"

# Default constants
DEFAULT_REQUEST_OFFSET = 0
DEFAULT_REQUEST_COUNT = 10
DEFAULT_REQUEST_MAX_PER_GROUP = 10
DEFAULT_FACET_MINHIT = 1
DEFAULT_FACET_MAXHIT = 10
DEFAULT_FACET_ORDER = PARAM_FACET_ORDER_HITS

#
# Utilities for result display
#

def print_line(keys, max_lens, char='-', sep_char='+'):
  sys.stdout.write(sep_char)
  for key in keys:
    sys.stdout.write(char * (max_lens[key] + 2) + sep_char)
  sys.stdout.write('\n')

def print_header(keys, max_lens, char='-', sep_char='+'):
  print_line(keys, max_lens, char=char, sep_char=sep_char)
  sys.stdout.write('|')
  for key in keys:
    sys.stdout.write(' %s%s |' % (key, ' ' * (max_lens[key] - len(key))))
  sys.stdout.write('\n')
  print_line(keys, max_lens, char=char, sep_char=sep_char)

def print_footer(keys, max_lens, char='-', sep_char='+'):
  print_line(keys, max_lens, char=char, sep_char=sep_char)

def safe_str(obj):
  """Return the byte string representation of obj."""
  try:
    return str(obj)
  except UnicodeEncodeError:
    # obj is unicode
    return unicode(obj).encode("unicode_escape")


class SenseiClientError(Exception):
  """Exception raised for all errors related to Sensei client."""

  def __init__(self, value):
    self.value = value

  def __str__(self):
    return repr(self.value)


class SenseiFacet:
  def __init__(self,expand=False,minHits=1,maxCounts=10,orderBy=PARAM_FACET_ORDER_HITS):
    self.expand = expand
    self.minHits = minHits
    self.maxCounts = maxCounts
    self.orderBy = orderBy

class SenseiSelections:
  def __init__(self, type):
    self.type = type;
    self.selection = {}
    
  def get_type(self):
    return self.type
  
  def get_selection(self):
    return self.selection

class SenseiQuery:
  def __init__(self, type):
    self.type = type
    self.query = {}
    
  def get_type(self):
    return self.type
  
  def get_query(self):
    return self.query  

class SenseiQueryMatchAll(SenseiQuery):
  def __init__(self):
    SenseiQuery.__init__(self, "match_all")
    self.query={"match_all":{"boost":1.0}}
    
  def set_boost(self, boost):
    target = (self.query)["match_all"]
    target["boost"]=boost
      
class SenseiQueryIDs(SenseiQuery):
  def __init__(self, values, excludes):
    SenseiQuery.__init__(self, "ids")
    self.query={"ids" : {"values" : [], "excludes":[], "boost":1.0}}
    if isinstance(values, list) and isinstance(excludes, list):
      self.query = {"ids" : {"values" : values, "excludes":excludes, "boost":1.0}}

  def add_values(self, values):
    if self.query.has_key("ids"):
      values_excludes = self.query["ids"]
      if values_excludes.has_key("values"):
        orig_values =  values_excludes["values"]
        orig_set = set(orig_values)
        for new_value in values:
          if new_value not in orig_set:
            orig_values.append(new_value)

  def add_excludes(self, excludes):
    if self.query.has_key("ids"):
      values_excludes = self.query["ids"]
      if values_excludes.has_key("excludes"):
        orig_excludes = values_excludes["excludes"]
        orig_set = set(orig_excludes)
        for new_value in excludes:
          if new_value not in orig_set:
            orig_excludes.append(new_value)
            
  def set_boost(self, boost):
    target = (self.query)["ids"]
    target["boost"]=boost

class SenseiQueryString(SenseiQuery):
  def __init__(self, query):
    SenseiQuery.__init__(self, "query_string")
    self.query={"query_string":{"query":query, 
                                "default_field":"contents", 
                                "default_operator":"OR",
                                "allow_leading_wildcard":True,
                                "lowercase_expanded_terms":True,
                                "enable_position_increments":True,
                                "fuzzy_prefix_length":0,
                                "fuzzy_min_sim":0.5,
                                "phrase_slop":0,
                                "boost":1.0,
                                "auto_generate_phrase_queries":False,
                                "fields":[],
                                "use_dis_max":True,
                                "tie_breaker":0 
                                 }}
  
  def set_field(self, field):
    self.query["query_string"]["default_field"]=field
    
  def set_operator(self, operator):
    self.query["query_string"]["default_operator"]=operator

  def set_allow_leading_wildcard(self, allow_leading_wildcard):
    self.query["query_string"]["allow_leading_wildcard"]=allow_leading_wildcard
    
  def set_lowercase_expanded_terms(self, lowercase_expanded_terms):
    self.query["query_string"]["lowercase_expanded_terms"]=lowercase_expanded_terms
        
  def set_enable_position_increments(self, enable_position_increments):
    self.query["query_string"]["enable_position_increments"]=enable_position_increments
    
  def set_fuzzy_prefix_length(self, fuzzy_prefix_length):
    self.query["query_string"]["fuzzy_prefix_length"]=fuzzy_prefix_length
    
  def set_fuzzy_min_sim(self, fuzzy_min_sim):
    self.query["query_string"]["fuzzy_min_sim"]=fuzzy_min_sim
        
  def set_phrase_slop(self, phrase_slop):
    self.query["query_string"]["phrase_slop"]=phrase_slop
    
  def set_boost(self, boost):
    self.query["query_string"]["boost"]=boost
    
  def set_auto_generate_phrase_queries(self, auto_generate_phrase_queries):
    self.query["query_string"]["auto_generate_phrase_queries"]=auto_generate_phrase_queries
    
  def set_fields(self, fields):
    if isinstance(fields, list):
      self.query["query_string"]["fields"]=fields
    
  def set_use_dis_max(self, use_dis_max):
    self.query["query_string"]["use_dis_max"]=use_dis_max
        
  def set_tie_breaker(self, tie_breaker):
    self.query["query_string"]["tie_breaker"]=tie_breaker
                                
   
class SenseiQueryText(SenseiQuery):
  def __init__(self, message, operator, type):
    SenseiQuery.__init__(self, "text")
    self.query={"text":{"message":message, "operator":operator, "type":type}}   
    
class SenseiQueryTerm(SenseiQuery):
  def __init__(self, column, value):
    SenseiQuery.__init__(self, "term")
    self.query={"term":{column:{"value":value, "boost":1.0}}}
  
  def set_boost(self, boost):
    target = (self.query)["term"]
    for column, desc in target.iterms():
      desc["boost"]=boost
        
                  
class SenseiFilter:
  def __init__(self, type):
    self.type = type
    self.filter = {}
    
  def get_type(self):
    return self.type
  
  def get_filter(self):
    return self.filter  
  
    
class SenseiFilterIDs(SenseiFilter):
  def __init__(self, values, excludes):
    SenseiFilter.__init__(self, "ids")
    self.filter={"ids" : {"values" : [], "excludes":[]}}
    if isinstance(values, list) and isinstance(excludes, list):
      self.filter = {"ids" : {"values" : values, "excludes":excludes}}

  def add_values(self, values):
    if self.filter.has_key("ids"):
      values_excludes = self.filter["ids"]
      if values_excludes.has_key("values"):
        orig_values =  values_excludes["values"]
        orig_set = set(orig_values)
        for new_value in values:
          if new_value not in orig_set:
            orig_values.append(new_value)

  def add_excludes(self, excludes):
    if self.filter.has_key("ids"):
      values_excludes = self.filter["ids"]
      if values_excludes.has_key("excludes"):
        orig_excludes =  values_excludes["excludes"]
        orig_set = set(orig_excludes)
        for new_value in excludes:
          if new_value not in orig_set:
            orig_excludes.append(new_value)
            
class SenseiFilterBool(SenseiFilter):
  def __init__(self, must_filter=None, must_not_filter=None, should_filter=None):            
    SenseiFilter.__init__(self, "bool");
    self.filter = {"bool":{"must":{}, "must_not":{}, "should":{}}}
    if must_filter is not None and isinstance(must_filter, SenseiFilter):
      target = (self.filter)["bool"]
      target["must"]=must_filter
    if must_not_filter is not None and isinstance(must_not_filter, SenseiFilter):
      target = (self.filter)["bool"]
      target["must_not"]=must_not_filter  
    if should_filter is not None and isinstance(should_filter, SenseiFilter):
      target = (self.filter)["bool"]
      target["should"]=should_filter
      
class SenseiFilterAND(SenseiFilter):
  def __init__(self, filter_list):
    SenseiFilter.__init__(self, "and")
    self.filter={"and":[]}
    old_filter_list = (self.filter)["and"]
    if isinstance(filter_list, list):
      for new_filter in filter_list:
        if isinstance(new_filter, SenseiFilter):
          old_filter_list.append(new_filter.get_filter())  
          
class SenseiFilterOR(SenseiFilter):
  def __init__(self, filter_list):
    SenseiFilter.__init__(self, "or")
    self.filter={"or":[]}
    old_filter_list = (self.filter)["or"]
    if isinstance(filter_list, list):
      for new_filter in filter_list:
        if isinstance(new_filter, SenseiFilter):
          old_filter_list.append(new_filter.get_filter())              
    
class SenseiFilterTerm(SenseiFilter):
  def __init__(self, column, value, noOptimize=False):
    SenseiFilter.__init__(self, "term")
    self.filter={"term":{column:{"value": value, "_noOptimize":noOptimize}}} 
    

class SenseiFilterTerms(SenseiFilter):
  def __init__(self, column, values=None, excludes=None, operator="or", noOptimize=False):
    SenseiFilter.__init__(self, "terms")
    self.filter={"terms":{}}
    if values is not None and isinstance(values, list):
      if excludes is  not None and isinstance(excludes, list):
        # complicated mode
        self.filter={"terms":{column:{"values":values, "excludes":excludes, "operator":operator, "_noOptimize":noOptimize}}}
      else:
        self.filter={"terms":{column:values}}
        
class SenseiFilterRange(SenseiFilter):
  def __init__(self, column, from_val, to_val):
    SenseiFilter.__init__(self, "range")
    self.filter={"range":{column:{"from":from_val, "to":to_val, "_noOptimize":False}}}         

  def set_No_optimization(self, type, date_format=None):
    range = (self.filter)["range"]
    for key, value in range.items():
      if value is not None:
        value["_type"] = type
        value["_noOptimize"] = True
        if type == "date" and date_format is not None:
          value["_date_format"]=date_format
    
class SenseiFilterQuery(SenseiFilter):
  def __init__(self, query):
    SenseiFilter.__init__(self, "query")
    self.filter={"query":{}}
    if isinstance(query, SenseiQuery):
      self.filter={"query": query.get_query()}
      
class SenseiFilterSelection(SenseiFilter):
  def __init__(self, selection):
    SenseiFilter.__init__(self, "selection")
    self.filter = {"selection":{}}
    if isinstance(selection, SenseiSelections):
      self.filter={"selection":selection.get_selection()}        
    
    
    
    
class SenseiSelection:
  def __init__(self, field, operation=PARAM_SELECT_OP_OR):
    self.field = field
    self.operation = operation
    self.type = None
    self.values = []
    self.excludes = []
    self.properties = {}

  def __str__(self):
    return ("Selection:%s:%s:%s:%s" %
            (self.field, self.operation,
             ','.join(self.values), ','.join(self.excludes)))

  def _get_type(self, value):
    if isinstance(value, basestring) and RANGE_REGEX.match(value):
      return SELECTION_TYPE_RANGE
    else:
      return SELECTION_TYPE_SIMPLE
    
  def addSelection(self, value, isNot=False):
    val_type = self._get_type(value)
    if not self.type:
      self.type = val_type
    elif self.type != val_type:
      raise SenseiClientError("Value (%s) type mismatch for facet %s: "
                              % (value, self.field))
    if isNot:
      self.excludes.append(safe_str(value))
    else:
      self.values.append(safe_str(value))
  
  def removeSelection(self, value, isNot=False):
    if isNot:
      self.excludes.remove(safe_str(value))
    else:
      self.values.remove(safe_str(value))
  
  def addProperty(self, name, value):
    self.properties[name] = value
  
  def removeProperty(self, name):
    del self.properties[name]

  def getValues(self):
    return self.values

  def setValues(self, values):
    self.values = []
    if len(values) > 0:
      for value in values:
        self.addSelection(value)

  def getExcludes(self):
    return self.excludes

  def setExcludes(self, excludes):
    self.excludes = []
    if len(excludes) > 0:
      for value in excludes:
        self.addSelection(value, True)

  def getType(self):
    return self.type

  def setType(self, val_type):
    self.type = val_type

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
  def __init__(self, field, reverse=False):
    self.field = field
    self.dir = None
    if not (field == PARAM_SORT_SCORE or
            field == PARAM_SORT_SCORE_REVERSE or
            field == PARAM_SORT_DOC or
            field == PARAM_SORT_DOC_REVERSE):
      if reverse:
        self.dir = PARAM_SORT_DESC
      else:
        self.dir = PARAM_SORT_ASC

  def __str__(self):
    return self.build_sort_field()

  def build_sort_field(self):
    if self.dir:
      return self.field + ":" + self.dir
    else:
      return self.field

  def build_sort_spec(self):
    if self.dir:
      return {self.field: self.dir}
    elif self.field == PARAM_SORT_SCORE:
      return "_score"
    else:
      return self.field

class SenseiFacetInitParams:
  """FacetHandler initialization parameters."""

  def __init__(self):
    self.bool_map = {}
    self.int_map = {}
    self.long_map = {}
    self.string_map = {}
    self.byte_map = {}
    self.double_map = {}

  # Getters for param names for different types
  def get_bool_param_names(self):
    return self.bool_map.keys()

  def get_int_param_names(self):
    return self.int_map.keys()

  def get_long_param_names(self):
    return self.long_map.keys()

  def get_string_param_names(self):
    return self.string_map.keys()

  def get_byte_param_names(self):
    return self.byte_map.keys()
  
  def get_double_param_names(self):
    return self.double_map.keys()

  # Add param name, values
  def put_bool_param(self, key, value):
    if isinstance(value, list):
      self.bool_map[key] = value
    else:
      self.bool_map[key] = [value]

  def put_int_param(self, key, value):
    if isinstance(value, list):
      self.int_map[key] = value
    else:
      self.int_map[key] = [value]

  def put_long_param(self, key, value):
    if isinstance(value, list):
      self.long_map[key] = value
    else:
      self.long_map[key] = [value]

  def put_string_param(self, key, value):
    if isinstance(value, list):
      self.string_map[key] = value
    else:
      self.string_map[key] = [value]

  def put_byte_param(self, key, value):
    if isinstance(value, list):
      self.byte_map[key] = value
    else:
      self.byte_map[key] = [value]

  def put_double_param(self, key, value):
    if isinstance(value, list):
      self.double_map[key] = value
    else:
      self.double_map[key] = [value]

  # Getters of param value(s) based on param names
  def get_bool_param(self, key):
    return self.bool_map.get(key)

  def get_int_param(self, key):
    return self.int_map.get(key)

  def get_long_param(self, key):
    return self.long_map.get(key)

  def get_string_param(self, key):
    return self.string_map.get(key)

  def get_byte_param(self, key):
    return self.byte_map.get(key)

  def get_double_param(self, key):
    return self.double_map.get(key)


class SenseiFacetInfo:

  def __init__(self, name, runtime=False, props={}):
    self.name = name
    self.runtime = runtime
    self.props = props

  def get_name(self):
    return self.name

  def set_name(self, name):
    self.name = name

  def get_runtime(self):
    return self.runtime

  def set_runtime(self, runtime):
    self.runtime = runtime

  def get_props(self):
    return self.props

  def set_props(self, props):
    self.props = props


class SenseiNodeInfo:

  def __init__(self, id, partitions, node_link, admin_link):
    self.id = id
    self.partitions = partitions
    self.node_link = node_link
    self.admin_link = admin_link

  def get_id(self):
    return self.id

  def get_partitions(self):
    return self.partitions

  def get_node_link(self):
    return self.node_link

  def get_admin_link(self):
    return self.admin_link


class SenseiSystemInfo:

  def __init__(self, json_data):
    logger.debug("json_data = %s" % json_data)
    self.num_docs = int(json_data.get(PARAM_SYSINFO_NUMDOCS))
    self.last_modified = long(json_data.get(PARAM_SYSINFO_LASTMODIFIED))
    self.version = json_data.get(PARAM_SYSINFO_VERSION)
    self.facet_infos = []
    for facet in json_data.get(PARAM_SYSINFO_FACETS):
      facet_info = SenseiFacetInfo(facet.get(PARAM_SYSINFO_FACETS_NAME),
                                   facet.get(PARAM_SYSINFO_FACETS_RUNTIME),
                                   facet.get(PARAM_SYSINFO_FACETS_PROPS))
      self.facet_infos.append(facet_info)
    # TODO: get cluster_info
    self.cluster_info = None

  def display(self):
    """Display sysinfo."""

    keys = ["facet_name", "facet_type", "runtime", "column", "column_type", "depends"]
    max_lens = None
    # XXX add existing flags

    def get_max_lens(columns):
      max_lens = {}
      for column in columns:
        max_lens[column] = len(column)
      for facet_info in self.facet_infos:
        props = facet_info.get_props()

        tmp_len = len(facet_info.get_name())
        if tmp_len > max_lens["facet_name"]:
          max_lens["facet_name"] = tmp_len

        tmp_len = len(props.get("type"))
        if tmp_len > max_lens["facet_type"]:
          max_lens["facet_type"] = tmp_len

        # runtime can only contain "true" or "false", so len("runtime")
        # is big enough

        tmp_len = len(props.get("column"))
        if tmp_len > max_lens["column"]:
          max_lens["column"] = tmp_len

        tmp_len = len(props.get("column_type"))
        if tmp_len > max_lens["column_type"]:
          max_lens["column_type"] = tmp_len

        tmp_len = len(props.get("depends"))
        if tmp_len > max_lens["depends"]:
          max_lens["depends"] = tmp_len
      return max_lens

    max_lens = get_max_lens(keys)
    print_header(keys, max_lens)

    for facet_info in self.facet_infos:
      props = facet_info.get_props()
      sys.stdout.write('|')
      val = facet_info.get_name()
      sys.stdout.write(' %s%s |' % (val, ' ' * (max_lens["facet_name"] - len(val))))

      val = props.get("type")
      sys.stdout.write(' %s%s |' % (val, ' ' * (max_lens["facet_type"] - len(val))))

      val = facet_info.get_runtime() and "true" or "false"
      sys.stdout.write(' %s%s |' % (val, ' ' * (max_lens["runtime"] - len(val))))

      val = props.get("column")
      sys.stdout.write(' %s%s |' % (val, ' ' * (max_lens["column"] - len(val))))

      val = props.get("column_type")
      sys.stdout.write(' %s%s |' % (val, ' ' * (max_lens["column_type"] - len(val))))

      val = props.get("depends")
      sys.stdout.write(' %s%s |' % (val, ' ' * (max_lens["depends"] - len(val))))

      sys.stdout.write('\n')

    print_footer(keys, max_lens)

  def get_num_docs(self):
    return self.num_docs

  def set_num_docs(self, num_docs):
    self.num_docs = num_docs

  def get_last_modified(self):
    return self.last_modified

  def set_last_modified(self, last_modified):
    self.last_modified = last_modified

  def get_facet_infos(self):
    return self.facet_infos

  def set_facet_infos(self, facet_infos):
    self.facet_infos = facet_infos

  def get_version(self):
    return self.version

  def set_version(self, version):
    self.version = version

  def get_cluster_info(self):
    return self.cluster_info

  def set_cluster_info(self, cluster_info):
    self.cluster_info = cluster_info


class SenseiRequest:

  def __init__(self,
               bql_req=None,
               offset=DEFAULT_REQUEST_OFFSET,
               count=DEFAULT_REQUEST_COUNT,
               max_per_group=DEFAULT_REQUEST_MAX_PER_GROUP,
               facet_map=None):
    self.qParam = {}
    self.explain = False
    self.route_param = None
    self.prepare_time = 0       # Statement prepare time in milliseconds
    self.stmt_type = "unknown"

    if bql_req != None:
      assert(facet_map)
      time1 = datetime.now()    # XXX need to move to SenseiClient

      # ok, msg = bql_req.merge_selections()
      # if not ok:
      #   raise SenseiClientError(msg)

      self.stmt_type = bql_req.get_stmt_type()
      if self.stmt_type == "desc":
        self.index = bql_req.get_index()
      else:
        self.query = bql_req.get_query()
        self.offset = bql_req.get_offset() or offset
        self.count = bql_req.get_count() or count
        self.columns = bql_req.get_columns()
        self.sorts = bql_req.get_sorts()
        self.selections = bql_req.get_selections()
        self.filter = bql_req.get_filter()
        self.query_pred = bql_req.get_query_pred()
        self.facets = bql_req.get_facets()
        # PARAM_RESULT_HIT_STORED_FIELDS is a reserved column name.  If this
        # column is selected, turn on fetch_stored flag automatically.
        if (PARAM_RESULT_HIT_STORED_FIELDS in self.columns or
            bql_req.get_fetching_stored()):
          self.fetch_stored = True
        else:
          self.fetch_stored = False
        self.groupby = bql_req.get_groupby()
        self.max_per_group = bql_req.get_max_per_group() or max_per_group
        self.facet_init_param_map = bql_req.get_facet_init_param_map()
        delta = datetime.now() - time1
        self.prepare_time = delta.seconds * 1000 + delta.microseconds / 1000
        logger.debug("Prepare time: %sms" % self.prepare_time)
    else:
      self.query = None
      self.offset = offset
      self.count = count
      self.columns = []
      self.sorts = None
      self.selections = []
      self.filter = {}
      self.query_pred = {}
      self.facets = {}
      self.fetch_stored = False
      self.groupby = None
      self.max_per_group = max_per_group
      self.facet_init_param_map = {}

  def set_offset(self, offset):
    self.offset = offset
    
  def set_count(self, count):
    self.count = count
    
  def set_query(self, query):
    self.query = query
    
  def set_explain(self, explain):
    self.explain = explain
    
  def set_fetch_stored(self, fetch_stored):
    self.fetch_stored = fetch_stored
    
  def set_route_param(self, route_param):
    self.route_param = route_param
    
  def set_sorts(self, sorts):    
    self.sorts = sorts
    
  def append_sort(self, sort):
    if isinstance(sort, SenseiSort):
      if self.sorts is None:
        self.sorts = []
        self.sorts.append(sort)
      else:  
        self.sorts.append(sort)
    
  def set_filter(self, filter):
    self.filter = filter
    
  def set_selections(self, selections):
    self.selections = selections
    
  def append_term_selection(self, column, value):
    if self.selections is None:
      self.selections = []
    term_selection = {"term": {column : {"value" : value}}}
    self.selections.append(term_selection)
  
  def append_terms_selection(self, column, values, excludes, operator):
    if self.selections is None:
      self.selections = []
    terms_selection = {"terms": {column : {"value" : value}}}
    self.selections.append(term_selection)  
    
  def append_range_selection(self, column, from_str="*", to_str="*", include_lower=True, include_upper=True):
    if self.selections is None:
      self.selections = []
    range_selection = {"range":{column:{"to":to_str, "from":from_str, "include_lower":include_lower, "include_upper":include_upper}}}
    self.selections.append(range_selection)
        
  def append_path_selection(self, column, value, strict=False, depth=1):
    if self.selections is None:
      self.selections = []
    path_selection = {"path": {column : {"value":value, "strict":strict, "depth":depth}}}
    self.selections.append(path_selection)      
        
  def set_facets(self, facets):
    self.facets = facets
    
  def set_groupby(self, groupby):
    self.groupby = groupby
    
  def set_max_per_group(self, max_per_group):
    self.max_per_group = max_per_group
      
  def set_facet_init_param_map(self, facet_init_param_map):
    self.facet_init_param_map = facet_init_param_map
    
  def get_columns(self):
    return self.columns

  
class SenseiHit:
  def __init__(self):
    self.docid = None
    self.uid = None
    self.srcData = {}
    self.score = None
    self.explanation = None
    self.stored = None
  
  def load(self, jsonHit):
    self.docid = jsonHit.get(PARAM_RESULT_HIT_DOCID)
    self.uid = jsonHit.get(PARAM_RESULT_HIT_UID)
    self.score = jsonHit.get(PARAM_RESULT_HIT_SCORE)
    srcStr = jsonHit.get(PARAM_RESULT_HIT_SRC_DATA)
    self.explanation = jsonHit.get(PARAM_RESULT_HIT_EXPLANATION)
    self.stored = jsonHit.get(PARAM_RESULT_HIT_STORED_FIELDS)
    if srcStr:
      self.srcData = json.loads(srcStr)
    else:
      self.srcData = None
  

class SenseiResultFacet:
  value = None
  count = None
  selected = None
  
  def load(self,json):
    self.value=json.get(PARAM_RESULT_FACET_INFO_VALUE)
    self.count=json.get(PARAM_RESULT_FACET_INFO_COUNT)
    self.selected=json.get(PARAM_RESULT_FACET_INFO_SELECTED,False)

  
class SenseiResult:
  """Sensei search results for a query."""

  def __init__(self, json_data):
    logger.debug("json_data = %s" % json_data)
    self.jsonMap = json_data
    self.parsedQuery = json_data.get(PARAM_RESULT_PARSEDQUERY)
    self.totalDocs = json_data.get(PARAM_RESULT_TOTALDOCS, 0)
    self.time = json_data.get(PARAM_RESULT_TIME, 0)
    self.total_time = 0
    self.numHits = json_data.get(PARAM_RESULT_NUMHITS, 0)
    self.hits = json_data.get(PARAM_RESULT_HITS)
    self.error = json_data.get("error")
    map = json_data.get(PARAM_RESULT_FACETS)
    self.facetMap = {}
    if map:
      for k, v in map.items():
        facetList = []
        for facet in v:
          facetObj = SenseiResultFacet()
          facetObj.load(facet)
          facetList.append(facetObj)
        self.facetMap[k]=facetList

  def display(self, columns=['*'], max_col_width=40):
    """Print the results in SQL SELECT result format."""

    keys = []
    max_lens = None
    has_group_hits = False

    def get_max_lens(columns):
      max_lens = {}
      has_group_hits = False
      srcdata_subcols = []
      srcdata_subcols_selected = False

      for col in columns:
        max_lens[col] = len(col)
        if re.match('_srcdata\.', col):
          srcdata_subcols.append(col.split('.')[1])
      if len(srcdata_subcols) > 0:
        srcdata_subcols_selected = True

      for hit in self.hits:
        if srcdata_subcols_selected and hit.has_key('_srcdata'):
          srcdata_json = json.loads(hit.get('_srcdata'))
          for subcol in srcdata_subcols:
            new_col = '_srcdata.' + subcol
            if srcdata_json.has_key(subcol):
              hit[new_col] = srcdata_json[subcol]
            else:
              hit[new_col] = '<Not Found>'
        group_hits = [hit]
        if hit.has_key(GROUP_HITS):
          group_hits = hit.get(GROUP_HITS)
          has_group_hits = True
        for group_hit in group_hits:
          for col in columns:
            if group_hit.has_key(col):
              v = group_hit.get(col)
            else:
              v = '<Not Found>'
            if isinstance(v, list):
              v = ','.join([safe_str(item) for item in v])
            elif isinstance(v, (int, long, float)):
              v = str(v)
            value_len = len(v)
            if value_len > max_lens[col]:
              max_lens[col] = min(value_len, max_col_width)
      return max_lens, has_group_hits

    if not self.hits:
      print "No hit is found."
      return
    elif not columns:
      print "No column is selected."
      return

    if len(columns) == 1 and columns[0] == '*':
      keys = self.hits[0].keys()
      if GROUP_HITS in keys:
        keys.remove(GROUP_HITS)
      if GROUP_VALUE in keys:
        keys.remove(GROUP_VALUE)
      if PARAM_RESULT_HIT_SRC_DATA in keys:
        keys.remove(PARAM_RESULT_HIT_SRC_DATA)
    else:
      keys = columns

    max_lens, has_group_hits = get_max_lens(keys)

    print_header(keys, max_lens,
                 has_group_hits and '=' or '-',
                 has_group_hits and '=' or '+')

    # Print the results
    for hit in self.hits:
      group_hits = [hit]
      if hit.has_key(GROUP_HITS):
        group_hits = hit.get(GROUP_HITS)
      for group_hit in group_hits:
        sys.stdout.write('|')
        for key in keys:
          if group_hit.has_key(key):
            v = group_hit.get(key)
          else:
            v = '<Not Found>'
          if isinstance(v, list):
            v = ','.join([safe_str(item) for item in v])
          elif isinstance(v, (int, float, long)):
            v = str(v)
          else:
            # The value may contain unicode characters
            v = safe_str(v)
          if len(v) > max_col_width:
            v = v[:max_col_width]
          sys.stdout.write(' %s%s |' % (v, ' ' * (max_lens[key] - len(v))))
        sys.stdout.write('\n')
      if has_group_hits:
        print_line(keys, max_lens)

    print_footer(keys, max_lens,
                 has_group_hits and '=' or '-',
                 has_group_hits and '=' or '+')

    sys.stdout.write('%s %s%s in set, %s hit%s, %s total doc%s (server: %sms, total: %sms)\n' %
                     (len(self.hits),
                      has_group_hits and 'group' or 'row',
                      len(self.hits) > 1 and 's' or '',
                      self.numHits,
                      self.numHits > 1 and 's' or '',
                      self.totalDocs,
                      self.totalDocs > 1 and 's' or '',
                      self.time,
                      self.total_time
                      ))

    # Print facet information
    for facet, values in self.jsonMap.get(PARAM_RESULT_FACETS).iteritems():
      max_val_len = len(facet)
      max_count_len = 1
      for val in values:
        max_val_len = max(max_val_len, min(max_col_width, len(val.get('value'))))
        max_count_len = max(max_count_len, len(str(val.get('count'))))
      total_len = max_val_len + 2 + max_count_len + 3

      sys.stdout.write('+' + '-' * total_len + '+\n')
      sys.stdout.write('| ' + facet + ' ' * (total_len - len(facet) - 1) + '|\n')
      sys.stdout.write('+' + '-' * total_len + '+\n')

      for val in values:
        sys.stdout.write('| %s%s (%s)%s |\n' %
                         (val.get('value'),
                          ' ' * (max_val_len - len(val.get('value'))),
                          val.get('count'),
                          ' ' * (max_count_len - len(str(val.get('count'))))))
      sys.stdout.write('+' + '-' * total_len + '+\n')
  
