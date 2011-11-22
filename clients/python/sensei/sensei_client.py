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

logger = logging.getLogger("sensei_client")

# Regular expression that matches a range facet value
RANGE_REGEX = re.compile(r'''\[(\d+(\.\d+)*|\*) TO (\d+(\.\d+)*|\*)\]''')

# Datetime regular expression
DATE_TIME = r'''(["'])(\d\d\d\d)([-/\.])(\d\d)\3(\d\d) (\d\d):(\d\d):(\d\d)\1'''
DATE_TIME_REGEX = re.compile(DATE_TIME)

# The lowest resolution that can make a difference in range predicate
EPSILON = 0.01

SELECTION_TYPE_RANGE = 1
SELECTION_TYPE_SIMPLE = 2
SELECTION_TYPE_TIME = 3

# TODO:
#
# 1. Term vector
# 2. Section

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

JSON_PARAM_COLUMN = "column"
JSON_PARAM_EXPLAIN = "explain"
JSON_PARAM_FACETS = "facets"
JSON_PARAM_FACET_INIT = "facetInit"
JSON_PARAM_FETCH_STORED = "fetchStored"
JSON_PARAM_FETCH_TERM_VECTORS = "fetchTermVectors"
JSON_PARAM_FILTERS = "filters"
JSON_PARAM_FROM = "from"
JSON_PARAM_GROUPBY = "groupBy"
JSON_PARAM_PARTITIONS = "partitions"
JSON_PARAM_QUERY = "query"
JSON_PARAM_QUERY_STRING = "query_string"
JSON_PARAM_ROUTEPARAM = "routeParam"
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
# Definition of the BQL statement grammar
#

from pyparsing import Literal, CaselessLiteral, Word, Upcase, delimitedList, Optional, \
    Combine, Group, alphas, nums, alphanums, ParseException, ParseFatalException, ParseSyntaxException, \
    Forward, oneOf, quotedString, \
    ZeroOrMore, restOfLine, Keyword, OnlyOnce, Suppress, removeQuotes, NotAny, OneOrMore, \
    MatchFirst, Regex, stringEnd, operatorPrecedence, opAssoc

"""

BNF Grammar for BQL
===================

<statement> ::= ( <select_stmt> | <describe_stmt> ) [';']

<select_stmt> ::= SELECT <select_list> <from_clause> [<where_clause>] [<given_clause>]
                  [<additional_clauses>]

<describe_stmt> ::= ( DESC | DESCRIBE ) <index_name>

<select_list> ::= '*' | <column_name_list>

<column_name_list> ::= <column_name> ( ',' <column_name> )*

<from_clause> ::= FROM <index_name>

<where_clause> ::= WHERE <search_expr>

# <search_expr> ::= <predicates>
#                 | <cumulative_predicates>
# 
<search_expr> ::= <predicate>
                | <search_expr> (AND | OR) <search_expr>
                | '(' <search_expr> ')'

<predicates> ::= <predicate> ( AND <predicate> )*

<predicate> ::= <in_predicate>
              | <contains_all_predicate>
              | <equal_predicate>
              | <not_equal_predicate>
              | <query_predicate>
              | <between_predicate>
              | <range_predicate>
              | <time_predicate>
              | <same_column_or_pred>

<in_predicate> ::= <column_name> [NOT] IN <value_list> [<except_clause>] [<predicate_props>]

<contains_all_predicate> ::= <column_name> CONTAINS ALL <value_list> [<except_clause>]
                             [<predicate_props>]

<equal_predicate> ::= <column_name> '=' <value> [<predicate_props>]

<not_equal_predicate> ::= <column_name> '<>' <value> [<predicate_props>]

<query_predicate> ::= QUERY IS <quoted_string>

<between_predicate> ::= <column_name> [NOT] BETWEEN <value> AND <value>

<range_predicate> ::= <column_name> <range_op> <numeric>

<time_predicate> ::= <column_name> IN LAST <time_span>
                   | <column_name> ( SINCE | AFTER | BEFORE ) <time_expr>

<same_column_or_pred> ::= '(' <cumulative_predicates> ')'

<cumulative_predicates> ::= <cumulative_predicate> ( OR <cumulative_predicate> )*

<cumulative_predicate> ::= <in_predicate>
                         | <equal_predicate>
                         | <between_predicate>
                         | <range_predicate>
                         | <time_predicate>

<value_list> ::= '(' <value> ( ',' <value> )* ')'

<value> ::= <quoted_string> | <numeric>

<range_op> ::= '<' | '<=' | '>=' | '>'

<except_clause> ::= EXCEPT <value_list>

<predicate_props> ::= WITH <prop_list>

<prop_list> ::= '(' <key_value_pair> ( ',' <key_value_pair> )* ')'

<key_value_pair> ::= <quoted_string> ':' <quoted_string>

<given_clause> ::= GIVEN FACET PARAM <facet_param_list>

<facet_param_list> ::= <facet_param> ( ',' <facet_param> )*

<facet_param> ::= '(' <facet_name> <facet_param_name> <facet_param_type> <facet_param_value> ')'

<facet_param_name> ::= <quoted_string>

<facet_param_type> ::= BOOLEAN | INT | LONG | STRING | BYTEARRAY | DOUBLE

<facet_param_value> ::= <quoted_string>

<additional_clauses> ::= ( <additional_clause> )+

<additional_clause> ::= <order_by_clause>
                      | <group_by_clause>
                      | <limit_clause>
                      | <browse_by_clause>
                      | <fetching_stored_clause>

<order_by_clause> ::= ORDER BY <sort_specs>

<sort_specs> ::= <sort_spec> ( ',', <sort_spec> )*

<sort_spec> ::= <column_name> [<ordering_spec>]

<ordering_spec> ::= ASC | DESC

<group_by_clause> ::= GROUP BY <group_spec>

<group_spec> ::= <facet_name> [TOP <max_per_group>]

<limit_clause> ::= LIMIT [<offset> ','] <count>

<offset> ::= ( <digit> )+

<count> ::= ( <digit> )+

<browse_by_clause> ::= BROWSE BY <facet_specs>

<facet_specs> ::= <facet_spec> ( ',' <facet_spec> )*

<facet_spec> ::= <facet_name> [<facet_expression>]

<facet_expression> ::= '(' <expand_flag> <count> <count> <facet_ordering> ')'

<expand_flag> ::= TRUE | FALSE

<facet_ordering> ::= HITS | VALUE

<fetching_stored_clause> ::= FETCHING STORED [<fetching_flag>]

<fetching_flag> ::= TRUE | FALSE

<quoted_string> ::= '"' ( <char> )* '"'
                  | "'" ( <char> )* "'"

<identifier> ::= <identifier_start> ( <identifier_part> )*

<identifier_start> ::= <alpha> | '-' | '_'

<identifier_part> ::= <identifier_start> | <digit>

<column_name> ::= <identifier>

<facet_name> ::= <identifier>

<alpha> ::= <alpha_lower_case> | <alpha_upper_case>

<alpha_upper_case> ::= A | B | C | D | E | F | G | H | I | J | K | L | M | N | O
                     | P | Q | R | S | T | U | V | W | X | Y | Z

<alpha_lower_case> ::= a | b | c | d | e | f | g | h | i | j | k | l | m | n | o
                     | p | q | r | s | t | u | v | w | x | y | z

<digit> ::= 0 | 1 | 2 | 3 | 4 | 5 | 6 | 7 | 8 | 9

<numeric> ::= <time_expr> | <number>

<number> ::= <integer> | <real>

<integer> ::= ( <digit> )+

<real> ::= ( <digit> )+ '.' ( <digit> )+

<time_expr> ::= <time_span> AGO
              | <date_time_string>
              | NOW

<time_span> ::= [<time_week_part>] [<time_day_part>] [<time_hour_part>]
                [<time_minute_part>] [<time_second_part>] [<time_millisecond_part>]

<time_week_part> ::= <integer> ( 'week' | 'weeks' )

<time_day_part>  ::= <integer> ( 'day'  | 'days' )

<time_hour_part> ::= <integer> ( 'hour' | 'hours' )

<time_minute_part> ::= <integer> ( 'minute' | 'minutes' | 'min' | 'mins')

<time_second_part> ::= <integer> ( 'second' | 'seconds' | 'sec' | 'secs')

<time_millisecond_part> ::= <integer> ( 'millisecond' | 'milliseconds' | 'msec' | 'msecs')

<date_time_string> ::= <digit><digit><digit><digit> ('-' | '/' | '.') <digit><digit>
                       ('-' | '/' | '.') <digit><digit>
                       <digit><digit> ':' <digit><digit> ':' <digit><digit>

"""

def order_by_action(s, loc, tok):
  for order in tok[1:]:
    if (order[0] == PARAM_SORT_SCORE and len(order) > 1):
      raise ParseSyntaxException(ParseException(s, loc, '"ORDER BY %s" should not be followed by %s'
                                                % (PARAM_SORT_SCORE, order[1])))
def pred_type(pred):
  return pred.keys()[0]

def pred_field(pred):
  return pred.values()[0].keys()[0]

def accumulate_range_pred(field_map, pred):
  old_range = field_map.get(pred_field(pred))
  if not old_range:
    field_map[pred_field(pred)] = pred
  else:
    # XXX
    pass

def predicate_and_action(s, loc, tok):
  # print ">>> in predicate_and_action: tok = ", tok
  # [[{'term': {'a': 1}}, 'and', {'term': {'b': 2}}, 'and', {'query_string': {'query': 'xxx'}}, 'and', {'term': {'c': 3}}]]

  filters = []
  field_map = {}
  for i in xrange(0, len(tok[0]), 2):
    pred = tok[0][i]
    if pred_type(pred) != "range":
      filters.append(pred)
    else:
      accumulate_range_pred(field_map, pred)
  for f in field_map:
    filters.append(f)
  return {"and": filters}

def predicate_or_action(s, loc, tok):
  # print ">>> in predicate_or_action: tok = ", tok
  filters = []
  for i in xrange(0, len(tok[0]), 2):
    if type(tok[0][i]) != dict:
      preds = tok[0][i].asList()
      filters.append(preds[0])
    else:
      filters.append(tok[0][i])
  # print ">>> in predicate_or_action: return ", {"or": filters}
  return {"or": filters}

def prop_list_action(s, loc, tok):
  props = {}
  for i in xrange(0, len(tok), 2):
    props[tok[i]] = tok[i+1]
  return props

def in_predicate_action(s, loc, tok):
  is_not = tok[1] == NOT.match
  if not is_not:
    return {"terms":
              {tok[0]: {
                 JSON_PARAM_VALUES: (tok.value_list[:] or []),
                 JSON_PARAM_EXCLUDES: (tok.except_values[:] or []),
                 JSON_PARAM_OPERATOR: PARAM_SELECT_OP_OR,
                 JSON_PARAM_NO_OPTIMIZE: False
                 }
               }
            }
  else:
    return {"terms":
              {tok[0]: {
                 JSON_PARAM_VALUES: (tok.except_values[:] or []),
                 JSON_PARAM_EXCLUDES: (tok.value_list[:] or []),
                 JSON_PARAM_OPERATOR: PARAM_SELECT_OP_OR,
                 JSON_PARAM_NO_OPTIMIZE: False
                 }
               }
            }

def query_predicate_action(s, loc, tok):
  # print ">>> in query_predicate_action: tok = ", tok
  return {JSON_PARAM_QUERY_STRING: {JSON_PARAM_QUERY: tok[2]}}

def equal_predicate_action(s, loc, tok):
  return {"term": {tok[0]: tok[2]}}

def range_predicate_action(s, loc, tok):
  # print ">>> in range_predicate_action: tok = ", tok
  val = None
  try:
    val = int(tok[2])
  except:
    val = float(tok[2])
  include_lower = (tok[1] == ">=")
  include_upper = (tok[1] == "<=")
  from_val = "*"
  to_val = "*"
  if tok[1] == "<" or tok[1] == "<=":
    to_val = val
  else:
    from_val = val
  return {"range":
            {tok[0]:
               {"from": from_val,
                "to": to_val,
                "include_lower": include_lower,
                "include_upper": include_upper
                }
             }
          }

limit_once = OnlyOnce(lambda s, loc, tok: tok)
order_by_once = OnlyOnce(order_by_action)
group_by_once = OnlyOnce(lambda s, loc, tok: tok)
browse_by_once = OnlyOnce(lambda s, loc, tok: tok)
fetching_stored_once = OnlyOnce(lambda s, loc, tok: tok)

def reset_all():
  limit_once.reset()
  order_by_once.reset()
  group_by_once.reset()
  browse_by_once.reset()
  fetching_stored_once.reset()

def convert_time(s, loc, toks):
  """Convert a time expression into an epoch time."""

  if toks[0] == NOW.match:
    return time_now
  elif toks.date_time_regex:
    mm = DATE_TIME_REGEX.match(toks[0])
    (_, year, _, month, day, hour, minute, second) = mm.groups()
    try:
      time_stamp = datetime.strptime("%s-%s-%s %s:%s:%s" % (year, month, day, hour, minute, second),
                                     "%Y-%m-%d %H:%M:%S")
    except ValueError as err:
      raise ParseSyntaxException(ParseException(s, loc, "Invalid date/time string: %s" % toks[0]))
    return int(time.mktime(time_stamp.timetuple()) * 1000)

def convert_time_span(s, loc, toks):
  """Convert a time span expression into an epoch time."""

  total = 0
  if toks.week_part:
    total += toks.week_part[0] * 7 * 24 * 60 * 60 * 1000
  if toks.day_part:
    total += toks.day_part[0] * 24 * 60 * 60 * 1000
  if toks.hour_part:
    total += toks.hour_part[0] * 60 * 60 * 1000
  if toks.minute_part:
    total += toks.minute_part[0] * 60 * 1000
  if toks.second_part:
    total += toks.second_part[0] * 1000
  if toks.millisecond_part:
    total += toks.millisecond_part[0]
  
  return time_now - total

#
# BQL tokens
#
# Remember to use lower case in the definition because we use
# Keyword.match to do comparison at other places in the code.
#
ALL = Keyword("all", caseless=True)
AFTER = Keyword("after", caseless=True)
AGO = Keyword("ago", caseless=True)
AND = Keyword("and", caseless=True)
ASC = Keyword("asc", caseless=True)
BEFORE = Keyword("before", caseless=True)
BETWEEN = Keyword("between", caseless=True)
BOOLEAN = Keyword("boolean", caseless=True)
BROWSE = Keyword("browse", caseless=True)
BY = Keyword("by", caseless=True)
BYTEARRAY = Keyword("bytearray", caseless=True)
CONTAINS = Keyword("contains", caseless=True)
DESC = Keyword("desc", caseless=True)
DESCRIBE = Keyword("describe", caseless=True)
DOUBLE = Keyword("double", caseless=True)
EXCEPT = Keyword("except", caseless=True)
FACET = Keyword("facet", caseless=True)
FALSE = Keyword("false", caseless=True)
FETCHING = Keyword("fetching", caseless=True)
FROM = Keyword("from", caseless=True)
GROUP = Keyword("group", caseless=True)
GIVEN = Keyword("given", caseless=True)
HITS = Keyword("hits", caseless=True)
IN = Keyword("in", caseless=True)
INT = Keyword("int", caseless=True)
IS = Keyword("is", caseless=True)
LAST = Keyword("last", caseless=True)
LIMIT = Keyword("limit", caseless=True)
LONG = Keyword("long", caseless=True)
NOT = Keyword("not", caseless=True)
NOW = Keyword("now", caseless=True)
OR = Keyword("or", caseless=True)
ORDER = Keyword("order", caseless=True)
PARAM = Keyword("param", caseless=True)
QUERY = Keyword("query", caseless=True)
SELECT = Keyword("select", caseless=True)
SINCE = Keyword("since", caseless=True)
STORED = Keyword("stored", caseless=True)
STRING = Keyword("string", caseless=True)
TOP = Keyword("top", caseless=True)
TRUE = Keyword("true", caseless=True)
VALUE = Keyword("value", caseless=True)
WHERE = Keyword("where", caseless=True)
WITH = Keyword("with", caseless=True)

keyword = MatchFirst((ALL, AND, ASC, BETWEEN, BOOLEAN, BROWSE, BY, BYTEARRAY,
                      CONTAINS, DESC, DESCRIBE, DOUBLE, EXCEPT,
                      FACET, FALSE, FETCHING, FROM, GROUP, GIVEN,
                      HITS, IN, INT, IS, LIMIT, LONG, NOT,
                      OR, ORDER, PARAM, QUERY,
                      SELECT, STORED, STRING, TOP, TRUE,
                      VALUE, WHERE, WITH
                      ))

LPAR, RPAR, COMMA, COLON, SEMICOLON = map(Suppress,"(),:;")
EQUAL = "="
NOT_EQUAL = "<>"

select_stmt = Forward()

ident = Word(alphas, alphanums + "_-.$")
column_name = ~keyword + Word(alphas, alphanums + "_-.")
facet_name = column_name.copy()
column_name_list = Group(delimitedList(column_name))

integer = Word(nums).setParseAction(lambda t: int(t[0]))
real = Combine(Word(nums) + "." + Word(nums)).setParseAction(lambda t: float(t[0]))
quotedString.setParseAction(removeQuotes)

# Time expression
CL = CaselessLiteral
week = Combine(CL("week") + Optional(CL("s")))
day = Combine(CL("day") + Optional(CL("s")))
hour = Combine(CL("hour") + Optional(CL("s")))
minute = Combine((CL("minute") | CL("min")) + Optional(CL("s")))
second = Combine((CL("second") | CL("sec")) + Optional(CL("s")))
millisecond = Combine((CL("millisecond") | CL("msec")) + Optional(CL("s")))

time_week_part = (integer + week).setResultsName("week_part")
time_day_part = (integer + day).setResultsName("day_part")
time_hour_part = (integer + hour).setResultsName("hour_part")
time_minute_part = (integer + minute).setResultsName("minute_part")
time_second_part = (integer + second).setResultsName("second_part")
time_millisecond_part = (integer + millisecond).setResultsName("millisecond_part")

time_span = (Optional(time_week_part) +
             Optional(time_day_part) +
             Optional(time_hour_part) +
             Optional(time_minute_part) +
             Optional(time_second_part) +
             Optional(time_millisecond_part)).setParseAction(convert_time_span)

date_time_string = Regex(DATE_TIME).setResultsName("date_time_regex").setParseAction(convert_time)

time_expr = ((time_span + AGO)
             | date_time_string
             | NOW.setParseAction(convert_time))

number = (real | integer)       # Put real before integer to avoid ambiguity
numeric = (time_expr | number)

value = (numeric | quotedString)
value_list = LPAR + delimitedList(value) + RPAR

prop_pair = (quotedString + COLON + value)
predicate_props = (WITH + LPAR + delimitedList(prop_pair).
                   setResultsName("prop_list").setParseAction(prop_list_action) + RPAR)

in_predicate = (column_name + Optional(NOT) +
                IN + value_list.setResultsName("value_list") +
                Optional(EXCEPT + value_list.setResultsName("except_values")) +
                Optional(predicate_props)
                ).setResultsName("in_pred").setParseAction(in_predicate_action)

contains_all_predicate = (column_name +
                          CONTAINS + ALL + value_list.setResultsName("value_list") +
                          Optional(EXCEPT + value_list.setResultsName("except_values")) +
                          Optional(predicate_props)
                          ).setResultsName("contains_all_pred")

equal_predicate = (column_name +
                   EQUAL + value +
                   Optional(predicate_props)
                   ).setResultsName("equal_pred").setParseAction(equal_predicate_action)

not_equal_predicate = (column_name +
                       NOT_EQUAL + value +
                       Optional(predicate_props)).setResultsName("not_equal_pred")

query_predicate = (QUERY + IS + quotedString
                   ).setResultsName("query_pred").setParseAction(query_predicate_action)

between_predicate = (column_name + Optional(NOT) +
                     BETWEEN + value + AND + value).setResultsName("between_pred")

range_op = oneOf("< <= >= >")
range_predicate = (column_name + range_op + numeric
                   ).setResultsName("range_pred").setParseAction(range_predicate_action)

time_predicate = ((column_name + IN + LAST + time_span)
                  | (column_name + (SINCE | AFTER | BEFORE) + time_expr)
                  ).setResultsName("time_pred")

cumulative_predicate = Group(in_predicate
                             | equal_predicate
                             | between_predicate
                             | range_predicate
                             ).setResultsName("cumulative_preds", listAllMatches=True)

cumulative_predicates = (cumulative_predicate +
                         OneOrMore(OR + cumulative_predicate))

same_column_or_pred = (LPAR + cumulative_predicates + RPAR).setResultsName("same_column_or_pred")

predicate = (in_predicate
             | contains_all_predicate
             | equal_predicate
             | not_equal_predicate
             | query_predicate
             | between_predicate
             | range_predicate
             | time_predicate
             | same_column_or_pred
             )

predicates = predicate + NotAny(OR) + ZeroOrMore(AND + predicate)

# search_condition = Group(predicates | cumulative_predicates)

search_expr = operatorPrecedence(predicate,
                                 [(AND, 2, opAssoc.LEFT, predicate_and_action),
                                  (OR,  2, opAssoc.LEFT, predicate_or_action)
                                  ])

param_type = BOOLEAN | INT | LONG | STRING | BYTEARRAY | DOUBLE
facet_param = Group(LPAR + facet_name + COMMA + quotedString + COMMA +
                    param_type + COMMA + value + RPAR).setResultsName("facet_param", listAllMatches=True)
given_clause = (GIVEN + FACET + PARAM + delimitedList(facet_param))

orderseq = ASC | DESC

order_by_expression = Forward()
order_by_spec = Group(column_name + Optional(orderseq)).setResultsName("orderby_spec", listAllMatches=True)
order_by_expression << (order_by_spec + ZeroOrMore(COMMA + order_by_expression))
order_by_clause = (ORDER + BY + order_by_expression).setResultsName("orderby").setParseAction(order_by_once)

limit_clause = (LIMIT + Group(Optional(integer + COMMA) + integer)).setResultsName("limit").setParseAction(limit_once)

expand_flag = TRUE | FALSE
facet_order_by = HITS | VALUE
facet_spec = Group(column_name +
                   Optional(LPAR + expand_flag + COMMA + integer  + COMMA + integer + COMMA + facet_order_by + RPAR))

group_by_clause = (GROUP + BY +
                   column_name.setResultsName("groupby") +
                   Optional(TOP + integer.setResultsName("max_per_group"))).setParseAction(group_by_once)

browse_by_clause = (BROWSE + BY +
                    delimitedList(facet_spec).setResultsName("facet_specs")).setParseAction(browse_by_once)

fetching_flag = TRUE | FALSE
fetching_stored_clause = (FETCHING + STORED +
                          Optional(fetching_flag)).setResultsName("fetching_stored").setParseAction(fetching_stored_once)

additional_clause = (order_by_clause
                     | limit_clause
                     | group_by_clause
                     | browse_by_clause
                     | fetching_stored_clause
                     )

additional_clauses = ZeroOrMore(additional_clause)

select_stmt << (SELECT + 
                ('*' | column_name_list).setResultsName("columns") + 
                FROM + 
                ident.setResultsName("index") + 
                Optional(WHERE + search_expr.setResultsName("where")) +
                Optional(given_clause.setResultsName("given")) +
                additional_clauses
                )

describe_stmt = (DESC | DESCRIBE).setResultsName("describe") + ident.setResultsName("index")

time_now = int(time.time() * 1000)
BQLstmt = (select_stmt | describe_stmt) + Optional(SEMICOLON) + stringEnd

# Define comment format, and ignore them
sql_comment = "--" + restOfLine
BQLstmt.ignore(sql_comment)


def safe_str(obj):
  """Return the byte string representation of obj."""
  try:
    return str(obj)
  except UnicodeEncodeError:
    # obj is unicode
    return unicode(obj).encode("unicode_escape")

def merge_values(list1, list2):
  """Merge two selection value lists and dedup.

  All selection values should be simple value types.

  """

  tmp = list1[:]
  if not tmp:
    return list2
  else:
    tmp.extend(list2)
    return list(set(tmp))

def and_ranges(range1, range2):
  """Try to AND two ranges.

  Return the intersection of two ranges if there is overlap; None otherwise.

  """
  def __max(n1, n2):
    if n1 == '*':
      return n2
    elif n2 == '*':
      return n1
    else:
      val1 = val2 = None
      try:
        val1 = int(n1)
      except:
        val1 = float(n1)
      try:
        val2 = int(n2)
      except:
        val2 = float(n2)
      return str(max(val1, val2))

  def __min(n1, n2):
    if n1 == '*':
      return n2
    elif n2 == '*':
      return n1
    else:
      val1 = val2 = None
      try:
        val1 = int(n1)
      except:
        val1 = float(n1)
      try:
        val2 = int(n2)
      except:
        val2 = float(n2)
      return str(min(val1, val2))

  m1 = RANGE_REGEX.match(range1)
  (low1, _, high1, _) = m1.groups()
  m2 = RANGE_REGEX.match(range2)
  (low2, _, high2, _) = m2.groups()

  low = __max(low1, low2)
  high = __min(high1, high2)

  if (low != '*' and high != '*'
      and float(low) > float(high)):
    return None
  else:
    return "[%s TO %s]" % (low, high)

def and_range_list(range_list, range0):
  new_list = []
  if not range_list:
    return new_list
  for r in range_list:
    new_r = and_ranges(r, range0)
    if new_r:
      new_list.append(new_r)
    else:
      return []
  return new_list

def collapse_cumulative_preds(cumulative_preds):
  """Collapse cumulative predicates into one selection."""

  # XXX Need to consider props here too
  selection = None
  selection_list = []
  field = None
  for pred in cumulative_preds:
    tmp = build_selection(pred)
    if not field and tmp:
      field = tmp.field
    elif tmp.field != field:
      raise SenseiClientError("A different column '%s' appeared in cumulative predicates"
                              % tmp.field)
    elif tmp.excludes:
      raise SenseiClientError("Negative predicate for column '%s' appeared in cumulative predicates"
                              % tmp.field)
    selection_list.append(tmp)

  if not selection_list:
    selection = None
  elif len(selection_list) == 1:
    selection = selection_list[0]
  else:
    values = selection_list[0].getValues()
    selection = SenseiSelection(field, PARAM_SELECT_OP_OR)
    for i in xrange(1, len(selection_list)):
      values = merge_values(values, selection_list[i].getValues())
    selection.setValues(values)
  return selection

def build_selection(predicate):
  """Build a SenseiSelection based on a predicate."""

  selection = None
  if predicate.in_pred:
    selection = SenseiSelection(predicate[0], PARAM_SELECT_OP_OR)
    is_not = predicate[1] == NOT.match
    for val in predicate.value_list:
      selection.addSelection(val, is_not)
    for val in predicate.except_values:
      selection.addSelection(val, not is_not)
    for i in xrange(0, len(predicate.prop_list), 2):
      selection.addProperty(predicate.prop_list[i], predicate.prop_list[i+1])

  elif predicate.contains_all_pred:
    selection = SenseiSelection(predicate[0], PARAM_SELECT_OP_AND)
    for val in predicate.value_list:
      selection.addSelection(val)
    for val in predicate.except_values:
      selection.addSelection(val, True)
    for i in xrange(0, len(predicate.prop_list), 2):
      selection.addProperty(predicate.prop_list[i], predicate.prop_list[i+1])

  elif predicate.equal_pred:
    selection = SenseiSelection(predicate[0], PARAM_SELECT_OP_AND)
    selection.addSelection(predicate[2])
    for i in xrange(0, len(predicate.prop_list), 2):
      selection.addProperty(predicate.prop_list[i], predicate.prop_list[i+1])
  
  elif predicate.not_equal_pred:
    selection = SenseiSelection(predicate[0], PARAM_SELECT_OP_OR)
    selection.addSelection(predicate[2], True)
    for i in xrange(0, len(predicate.prop_list), 2):
      selection.addProperty(predicate.prop_list[i], predicate.prop_list[i+1])
  
  elif predicate.between_pred:
    if predicate[1] == BETWEEN.match:
      selection = SenseiSelection(predicate[0], PARAM_SELECT_OP_AND)
      selection.addSelection("[%s TO %s]" % (predicate[2], predicate[4]))
    else:
      selection = SenseiSelection(predicate[0], PARAM_SELECT_OP_AND)
      selection.addSelection("[%s TO %s]" % (predicate[3], predicate[5]), True)

  elif predicate.range_pred:
    low = "*"
    high = "*"
    delta = isinstance(predicate[2], int) and 1 or EPSILON
    if predicate[1] == "<":
      high = max(predicate[2] - delta, 0)
    elif predicate[1] == "<=":
      high = predicate[2]
    elif predicate[1] == ">=":
      low = predicate[2]
    else:
      low = predicate[2] + delta
    selection = SenseiSelection(predicate[0], PARAM_SELECT_OP_AND)
    selection.addSelection("[%s TO %s]" % (low, high))

  elif predicate.time_pred:
    selection = SenseiSelection(predicate[0], PARAM_SELECT_OP_AND)
    if predicate[1] == IN.match and predicate[2] == LAST.match:
      selection.addSelection("[%s TO %s]" % (predicate[3], "*"))
    elif predicate[1] == SINCE.match or predicate[1] == AFTER.match:
      selection.addSelection("[%s TO %s]" % (predicate[2] + 1, "*"))
    elif predicate[1] == BEFORE.match:
      selection.addSelection("[%s TO %s]" % ("*", predicate[2] - 1))
  
  elif predicate.same_column_or_pred:
    selection = collapse_cumulative_preds(predicate.cumulative_preds)

  return selection


def build_filter(predicate):
  """Build a filter based on a predicate."""

  filter = None
  if predicate.in_pred:
    is_not = predicate[1] == NOT.match
    if not is_not:
      return {"facetSelection":
                {predicate[0]: {
                   "value": predicate.value_list,
                   "excludes": predicate.except_values,
                   "operator": PARAM_SELECT_OP_OR,
                   "params": predicate.prop_list
                   }
                 }
              }
    else:
      return {"facetSelection":
                {predicate[0]: {
                   "value": predicate.except_values,
                   "excludes": predicate.value_list,
                   "operator": PARAM_SELECT_OP_OR,
                   "params": predicate.prop_list
                   }
                 }
              }

  elif predicate.contains_all_pred:
    selection = SenseiSelection(predicate[0], PARAM_SELECT_OP_AND)
    for val in predicate.value_list:
      selection.addSelection(val)
    for val in predicate.except_values:
      selection.addSelection(val, True)
    for i in xrange(0, len(predicate.prop_list), 2):
      selection.addProperty(predicate.prop_list[i], predicate.prop_list[i+1])

  elif predicate.equal_pred:
    return {"facetSelection":
              {predicate[0]: {
                 "value": [predicate[2]],
                 "excludes": {},
                 "operator": PARAM_SELECT_OP_AND,
                 "params": predicate.prop_list
                 }
               }
            }

  return filter

class BQLRequest:
  """A Sensei request with a BQL statement.

  The BQL statement can be one of the following statements:

  1. SELECT
  2. DESCRIBE

  """

  def __init__(self, sql_stmt):
    try:
      time_now = int(time.time() * 1000)
      self.tokens = BQLstmt.parseString(sql_stmt, parseAll=True)
    except ParseException as err:
      raise err
    except ParseSyntaxException as err:
      raise err
    except ParseFatalException as err:
      raise err
    finally:
      reset_all()

    self.query = ""
    self.selections = None
    self.selection_list = []
    self.filters = None
    self.query_pred = None
    self.sorts = None
    self.columns = [safe_str(col) for col in self.tokens.columns]
    self.facet_init_param_map = None

    if self.tokens.describe:
      self.stmt_type = "desc"
    else:
      self.stmt_type = "select"

    where = self.tokens.where
    if where:
      if type(where) == dict:
        pass
      else:
        # Single predicate in where clause
        where = where.asList()[0]
      self.__extract_query_and_filter(where)

      # if where[0].get(JSON_PARAM_QUERY):
      #   self.query_pred = where[0].get(JSON_PARAM_QUERY)
      #   where[0].pop(JSON_PARAM_QUERY)
    
      # if where.predicates:
      #   for predicate in where.predicates:
      #     if predicate.query_pred:
      #       self.query = predicate[2]
      #     else:
      #       selection = build_selection(predicate)
      #       if selection:
      #         self.selection_list.append(selection)
      # elif where.cumulative_preds:
      #   selection = collapse_cumulative_preds(where.cumulative_preds)
      #   self.selection_list.append(selection)

  def __extract_query_and_filter(self, where):
    """Extract the query and filter information from the where clause."""

    if where.get(JSON_PARAM_QUERY_STRING):
      self.query_pred = where
      self.filter = None
    elif where.get("and"):
      preds = where.get("and")
      query_pred = None
      for pred in preds:
        if pred.get(JSON_PARAM_QUERY_STRING):
          query_pred = pred
          break
      if query_pred:
        self.query_pred = query_pred
        preds.remove(pred)
        if len(preds) == 1:
          self.filters = preds[0]
        else:
          self.filters = {"and": preds}
      else:
        self.filters = where
    else:
      self.filters = where

  def get_stmt_type(self):
    """Get the statement type."""

    return self.stmt_type

  def get_offset(self):
    """Get the offset."""

    limit = self.tokens.limit
    if limit:
      if len(limit[1]) == 2:
        return limit[1][0]
      else:
        return None
    else:
      return None

  def get_count(self):
    """Get the count (default 10)."""

    limit = self.tokens.limit
    if limit:
      if len(limit[1]) == 2:
        return limit[1][1]
      else:
        return limit[1][0]
    else:
      return None

  def get_index(self):
    """Get the index (i.e. table) name."""

    return self.tokens.index

  def get_columns(self):
    """Get the list of selected columns."""

    return self.columns

  def get_query(self):
    """Get the query string."""

    return self.query

  def get_sorts(self):
    """Get the SenseiSort array base on ORDER BY."""

    if self.sorts:
      return self.sorts

    self.sorts = []
    orderby = self.tokens.orderby
    if orderby:
      orderby_spec = orderby.orderby_spec
      for spec in orderby_spec:
        if len(spec) == 1:
          self.sorts.append(SenseiSort(spec[0]))
        else:
          self.sorts.append(SenseiSort(spec[0], spec[1] == "desc"))
    return self.sorts

  def merge_selections(self):
    """Merge all selections and detect conflicts."""

    self.selections = {}
    for selection in self.selection_list:
      existing = self.selections.get(selection.field)
      if existing:
        # Try to merge simple range predicates
        if (len(selection.getValues()) == 1 and
            selection.getType() == SELECTION_TYPE_RANGE and
            existing.getType() == SELECTION_TYPE_RANGE):
          new_values = and_range_list(existing.getValues(), selection.getValues()[0])
          if not new_values:
            raise SenseiClientError("There is conflict in selection(s) for column '%s'" % selection.field)
          existing.setValues(new_values)
        else:
          # Don't bother trying to merge predicates
          if existing.getValues() and selection.getValues():
            return False, "There is conflict in selection(s) for column '%s'" % selection.field
          if selection.getValues():
            existing.setValues(selection.getValues())
          if selection.getExcludes():
            existing.setExcludes(merge_values(existing.getExcludes(),
                                            selection.getExcludes()))
        # XXX How about props?
      else:
        self.selections[selection.field] = selection
    return True, None

  def get_selections(self):
    """Get all the selections from in statement."""

    if self.selections == None:
      self.merge_selections()
    return self.selections

  def get_filters(self):
    """Get the filters from the statement."""

    return self.filters

  def get_query_pred(self):
    """Get the QUERY predicate."""
    return self.query_pred

  def get_facets(self):
    """Get facet specs."""

    facet_specs = self.tokens.facet_specs
    if not facet_specs:
      return {}
    facets = {}
    for spec in facet_specs:
      facet = None
      if len(spec) == 1:
        facet = SenseiFacet(False,
                            DEFAULT_FACET_MINHIT,
                            DEFAULT_FACET_MAXHIT,
                            DEFAULT_FACET_ORDER)
      else:
        facet = SenseiFacet(spec[1] == "true",
                            spec[2],
                            spec[3],
                            spec[4] == "hits" and PARAM_FACET_ORDER_HITS or PARAM_FACET_ORDER_VAL)
      facets[spec[0]] = facet
    return facets

  def get_groupby(self):
    """Get group by facet name."""

    if self.tokens.groupby:
      return self.tokens.groupby[0]
    else:
      return None

  def get_max_per_group(self):
    """Get max_per_group value."""

    if self.tokens.max_per_group:
      return self.tokens.max_per_group
    else:
      return None

  def get_fetching_stored(self):
    """Get the fetching-stored flag."""

    fetching_stored = self.tokens.fetching_stored
    if (not fetching_stored or
        len(fetching_stored) == 2 or
        fetching_stored[2] == "true"):
      return True
    else:
      return False

  def get_facet_init_param_map(self):
    """Get run-time facet handler initialization parameters."""

    if self.facet_init_param_map:
      return self.facet_init_param_map

    self.facet_init_param_map = {}
    given = self.tokens.given
    if given:
      for param in given.facet_param:
        facet = param[0]
        name = param[1]
        param_type = param[2]
        value = param[3]
        init_params = None

        if self.facet_init_param_map.has_key(facet):
          init_params = self.facet_init_param_map[facet]
        else:
          init_params = SenseiFacetInitParams()
          self.facet_init_param_map[facet] = init_params

        if param_type == "boolean":
          init_params.put_bool_param(name, value)
        elif param_type == "int":
          init_params.put_int_param(name, value)
        elif param_type == "long":
          init_params.put_long_param(name, value)
        elif param_type == "string":
          init_params.put_string_param(name, value)
        elif param_type == "bytearray":
          init_params.put_byte_param(name, value)
        elif param_type == "double":
          init_params.put_double_param(name, value)

    return self.facet_init_param_map


def test(str):
  try:
    tokens = BQLstmt.parseString(str)
    print "tokens =",        tokens
    print "tokens.columns =", tokens.columns
    print "tokens.index =",  tokens.index
    print "tokens.where =", tokens.where
    if tokens.where:
      pass
      # print "tokens.where.predicates =", tokens.where.predicates
      # print "tokens.where.cumulative_preds =", tokens.where.cumulative_preds
      # for predicate in tokens.where.predicates:
      #   print "--------------------------------------"
      #   print "predicate.value_list =", predicate.value_list
      #   print "predicate.except_values =", predicate.except_values
      #   print "predicate.prop_list =", predicate.prop_list
      #   if predicate.cumulative_preds:
      #     print "predicate.cumulative_preds =", predicate.cumulative_preds
    print "tokens.orderby =", tokens.orderby
    if tokens.orderby:
      print "tokens.orderby.orderby_spec =", tokens.orderby.orderby_spec
    print "tokens.limit =", tokens.limit
    print "tokens.facet_specs =", tokens.facet_specs
    print "tokens.groupby =", tokens.groupby
    print "tokens.max_per_group =", tokens.max_per_group
    print "tokens.given =", tokens.given
    if tokens.given:
      print "tokens.given.facet_param =", tokens.given.facet_param
    print "tokens.fetching_stored =", tokens.fetching_stored
  except ParseException as err:
    # print " " * (err.loc + 2) + "^\n" + err.msg
    pass
  except ParseSyntaxException as err:
    # print " " * (err.loc + 2) + "^\n" + err.msg
    pass
  except ParseFatalException as err:
    # print " " * (err.loc + 2) + "^\n" + err.msg
    pass
  finally:
    reset_all()

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

  def __get_type(self, value):
    if isinstance(value, basestring) and RANGE_REGEX.match(value):
      return SELECTION_TYPE_RANGE
    else:
      return SELECTION_TYPE_SIMPLE
    
  def addSelection(self, value, isNot=False):
    val_type = self.__get_type(value)
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

    def print_line(char='-', sep_char='+'):
      sys.stdout.write(sep_char)
      for key in keys:
        sys.stdout.write(char * (max_lens[key] + 2) + sep_char)
      sys.stdout.write('\n')

    def print_header():
      print_line('-', '+')
      sys.stdout.write('|')
      for key in keys:
        sys.stdout.write(' %s%s |' % (key, ' ' * (max_lens[key] - len(key))))
      sys.stdout.write('\n')
      print_line('-', '+')

    def print_footer():
      print_line('-', '+')
      
    max_lens = get_max_lens(keys)
    print_header()

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

    print_footer()

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
               sql_stmt=None,
               offset=DEFAULT_REQUEST_OFFSET,
               count=DEFAULT_REQUEST_COUNT,
               max_per_group=DEFAULT_REQUEST_MAX_PER_GROUP):
    self.qParam = {}
    self.explain = False
    self.route_param = None
    self.sql_stmt = sql_stmt
    self.prepare_time = 0       # Statement prepare time in milliseconds
    self.stmt_type = "unknown"

    if sql_stmt != None:
      time1 = datetime.now()
      bql_req = BQLRequest(sql_stmt)
      ok, msg = bql_req.merge_selections()
      if not ok:
        raise SenseiClientError(msg)

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
        self.filters = bql_req.get_filters()
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
      self.selections = {}
      self.filters = {}
      self.query_pred = {}
      self.facets = {}
      self.fetch_stored = False
      self.groupby = None
      self.max_per_group = max_per_group
      self.facet_init_param_map = {}

  def get_columns(self):
    return self.columns

  
# XXX Do we really need this class?
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
      for col in columns:
        max_lens[col] = len(col)
      for hit in self.hits:
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

    def print_line(char='-', sep_char='+'):
      sys.stdout.write(sep_char)
      for key in keys:
        sys.stdout.write(char * (max_lens[key] + 2) + sep_char)
      sys.stdout.write('\n')

    def print_header():
      if has_group_hits:
        print_line('=', '=')
      else:
        print_line('-', '+')
      sys.stdout.write('|')
      for key in keys:
        sys.stdout.write(' %s%s |' % (key, ' ' * (max_lens[key] - len(key))))
      sys.stdout.write('\n')
      if has_group_hits:
        print_line('=', '=')
      else:
        print_line('-', '+')

    def print_footer():
      if has_group_hits:
        print_line('=', '=')
      else:
        print_line('-', '+')
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

    print_header()

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
        print_line()

    # Print the result footer
    print_footer()

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
  
class SenseiClient:
  """Sensei client class."""

  def __init__(self,host='localhost',port=8080,path='sensei'):
    self.host = host
    self.port = port
    self.path = path
    self.url = 'http://%s:%d/%s' % (self.host,self.port,self.path)
    self.opener = urllib2.build_opener()
    self.opener.addheaders = [('User-agent', 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_6_7) AppleWebKit/534.30 (KHTML, like Gecko) Chrome/12.0.742.91 Safari/534.30')]
    
  @staticmethod
  def buildJsonString(req, sort_keys=True, indent=None):
    """
    Build a Sensei request in JSON format.

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

    if req.filters:
      output_json[JSON_PARAM_FILTERS] = req.filters
    if req.query_pred:
      output_json[JSON_PARAM_QUERY] = req.query_pred

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
      output_json[JSON_PARAM_GROUPBY] = [{
        JSON_PARAM_COLUMN: req.groupby,
        JSON_PARAM_TOP: req.max_per_group
        }]

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
    
  def doQuery(self, req=None, using_json=True):
    """Execute a search query."""

    time1 = datetime.now()
    query_string = None
    if using_json: # Use JSON format
      query_string = SenseiClient.buildJsonString(req)
    else:
      query_string = SenseiClient.buildUrlString(req)
    logger.debug(query_string)
    # urlReq = urllib2.Request(self.url, query_string)
    # res = self.opener.open(urlReq)
    # line = res.read()
    # jsonObj = json.loads(line)
    # res = SenseiResult(jsonObj)
    # delta = datetime.now() - time1
    # res.total_time = delta.seconds * 1000 + delta.microseconds / 1000
    # return res

  def getSystemInfo(self):
    """Get Sensei system info."""

    urlReq = urllib2.Request(self.url + "/sysinfo")
    res = self.opener.open(urlReq)
    line = res.read()
    jsonObj = json.loads(line)
    res = SenseiSystemInfo(jsonObj)
    return res

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
      if options.verbose:
        test(stmt)
      req = SenseiRequest(stmt)
      if req.stmt_type == "select":
        res = client.doQuery(req)
        # res.display(columns=req.get_columns(), max_col_width=int(options.max_col_width))
      elif req.stmt_type == "desc":
        sysinfo = client.getSystemInfo()
        sysinfo.display()
      else:
        pass
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


"""
Testing Data:

select color, year, tags, price from cars where query is "cool" and color in ("gold", "green", "blue") except ("black", "blue", "yellow", "white", "red", "silver") and year in ("[1996 TO 1997]", "[2002 TO 2003]") order by price desc limit 0,10
+-------+----------------------+----------------------------------+-------------------------+
| color | year                 | tags                             | price                   |
+-------+----------------------+----------------------------------+-------------------------+
| gold  | 00000000000000001997 | cool,moon-roof,reliable,towing   | 00000000000000015000.00 |
| green | 00000000000000001996 | cool,favorite,reliable,towing    | 00000000000000015000.00 |
| green | 00000000000000001996 | cool,favorite,reliable,towing    | 00000000000000014800.00 |
| green | 00000000000000001996 | cool,moon-roof,reliable,towing   | 00000000000000014800.00 |
| green | 00000000000000002002 | automatic,cool,reliable,towing   | 00000000000000014800.00 |
| gold  | 00000000000000002002 | cool,favorite,navigation,towing  | 00000000000000014700.00 |
| gold  | 00000000000000001996 | cool,favorite,reliable,towing    | 00000000000000014700.00 |
| gold  | 00000000000000001997 | cool,favorite,reliable,towing    | 00000000000000014700.00 |
| gold  | 00000000000000001996 | cool,electric,moon-roof,reliable | 00000000000000014400.00 |
| gold  | 00000000000000001997 | cool,favorite,hybrid,reliable    | 00000000000000014200.00 |
+-------+----------------------+----------------------------------+-------------------------+
10 rows in set, 325 hits, 15001 total docs

select color, year, tags, price from cars where query is "cool" and tags contains all ("cool", "hybrid") except("favorite") and color in ("red", "yellow") order by price desc limit 0,5
+--------+----------------------+----------------------------------+-------------------------+
| color  | year                 | tags                             | price                   |
+--------+----------------------+----------------------------------+-------------------------+
| yellow | 00000000000000001995 | cool,hybrid,moon-roof,reliable   | 00000000000000014500.00 |
| red    | 00000000000000002000 | cool,hybrid,moon-roof,navigation | 00000000000000014500.00 |
| red    | 00000000000000001993 | cool,hybrid,moon-roof,navigation | 00000000000000014400.00 |
| red    | 00000000000000002002 | automatic,cool,hybrid,navigation | 00000000000000014200.00 |
| yellow | 00000000000000001999 | automatic,cool,hybrid,reliable   | 00000000000000012200.00 |
+--------+----------------------+----------------------------------+-------------------------+
5 rows in set, 132 hits, 15001 total docs

select color, year, tags, price from cars where query is "cool" and tags contains all ("cool", "hybrid") except ("favorite") and color in ("red") with ("aaa":"111", "bbb":"222") order by price desc limit 0,10 browse by color(true, 1, 10, hits), year(true, 1, 10, value), price(true, 1, 10, value)
+-------+----------------------+----------------------------------+-------------------------+
| color | year                 | tags                             | price                   |
+-------+----------------------+----------------------------------+-------------------------+
| red   | 00000000000000002000 | cool,hybrid,moon-roof,navigation | 00000000000000014500.00 |
| red   | 00000000000000001993 | cool,hybrid,moon-roof,navigation | 00000000000000014400.00 |
| red   | 00000000000000002002 | automatic,cool,hybrid,navigation | 00000000000000014200.00 |
| red   | 00000000000000001998 | automatic,cool,hybrid,navigation | 00000000000000012100.00 |
| red   | 00000000000000002002 | automatic,cool,hybrid,reliable   | 00000000000000011500.00 |
| red   | 00000000000000002002 | automatic,cool,hybrid,reliable   | 00000000000000011400.00 |
| red   | 00000000000000001998 | automatic,cool,hybrid,reliable   | 00000000000000011400.00 |
| red   | 00000000000000001996 | automatic,cool,hybrid,reliable   | 00000000000000011200.00 |
| red   | 00000000000000001999 | automatic,cool,hybrid,reliable   | 00000000000000011100.00 |
| red   | 00000000000000002001 | cool,hybrid,moon-roof,reliable   | 00000000000000010500.00 |
+-------+----------------------+----------------------------------+-------------------------+
10 rows in set, 59 hits, 15001 total docs
+-------------+
| color       |
+-------------+
| white  (73) |
| yellow (73) |
| blue   (62) |
| silver (61) |
| red    (59) |
| green  (58) |
| gold   (53) |
| black  (52) |
+-------------+
+-----------------------+
| price                 |
+-----------------------+
| [* TO 6700]      (21) |
| [10000 TO 13100] (8)  |
| [13200 TO 17300] (3)  |
| [6800 TO 9900]   (27) |
+-----------------------+
+---------------------+
| year                |
+---------------------+
| [1993 TO 1994] (16) |
| [1995 TO 1996] (13) |
| [1997 TO 1998] (10) |
| [1999 TO 2000] (9)  |
| [2001 TO 2002] (11) |
+---------------------+

select color, grouphitscount from cars group by color top 1 limit 0,16000
+--------+----------------+
| color  | grouphitscount |
+--------+----------------+
| white  | 2196           |
| yellow | 2105           |
| red    | 2160           |
| black  | 3141           |
| green  | 1085           |
| gold   | 1110           |
| blue   | 1104           |
| silver | 2100           |
+--------+----------------+
8 rows in set, 15001 hits, 15001 total docs

Note: (= (+ 2196 2105 2160 3141 1085 1110 1104 2100) 15001)

select uid, color, makemodel from cars group by color top 3 limit 0,4
=========================================
| uid | color  | makemodel              |
=========================================
| 1   | white  | asian/acura/1.6el      |
| 2   | white  | asian/acura/1.6el      |
| 3   | white  | asian/acura/1.6el      |
+-----+--------+------------------------+
| 0   | yellow | asian/acura/1.6el      |
| 244 | yellow | european/bentley/azure |
| 245 | yellow | european/bentley/azure |
+-----+--------+------------------------+
| 242 | red    | european/bentley/azure |
| 246 | red    | european/bentley/azure |
| 247 | red    | european/bentley/azure |
+-----+--------+------------------------+
| 241 | black  | european/bentley/azure |
| 243 | black  | european/bentley/azure |
| 10  | black  | asian/acura/3.2tl      |
+-----+--------+------------------------+
=========================================
4 groups in set, 15001 hits, 15001 total docs

// Signal search example
select tags, publicShareFlag, userid, country, updateType from signal where country in ("us") and My-Network in ("1", "2") given facet param (My-Network, "srcid", int, "8233570")

------------------------------------------------------------------------

select * from cars where color = "red" OR year = 1995 AND color in ("blue", "black")
select * from cars where (color = "red" OR color = "blue") OR year = 1995

"""
