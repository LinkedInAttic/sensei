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

#
# BQL Parser Implementation in Python
#

import json
import logging
import datetime
from datetime import datetime
import time
import re

from sensei_components import *

logger = logging.getLogger("bql_parser")

# Regular expression that matches a range facet value
RANGE_REGEX = re.compile(r'''\[(\d+(\.\d+)*|\*) TO (\d+(\.\d+)*|\*)\]''')

# Datetime regular expression
DATE_TIME = r'''(["'])(\d\d\d\d)([-/\.])(\d\d)\3(\d\d) (\d\d):(\d\d):(\d\d)\1'''
DATE_TIME_REGEX = re.compile(DATE_TIME)

# The lowest resolution that can make a difference in range predicate
EPSILON = 0.01

# TODO:
#
# 1. Term vector
# 2. Section

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

<select_stmt> ::= SELECT <select_list> [<from_clause>] [<where_clause>] [<given_clause>]
                  [<additional_clauses>]

<describe_stmt> ::= ( DESC | DESCRIBE ) [<index_name>]

<select_list> ::= '*' | <column_name_list>

<column_name_list> ::= <column_name> ( ',' <column_name> )*

<from_clause> ::= FROM <index_name>

<where_clause> ::= WHERE <search_expr>

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
              | <match_predicate>
              | <like_predicate>

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

<match_predicate> ::= MATCH '(' column_name_list ')' AGAINST '(' quoted_string ')'

<like_predicate> ::= <column_name> LIKE <quoted_string>

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

class BQLParser:
  """BQL Parser.

  This BQL parser takes a BQL statement string as input, and return parsed
  tokens.

  """

  def __init__(self, facet_map):
    self.limit_once = OnlyOnce(lambda s, loc, tok: tok)
    self.order_by_once = OnlyOnce(self.order_by_action)
    self.group_by_once = OnlyOnce(lambda s, loc, tok: tok)
    self.browse_by_once = OnlyOnce(lambda s, loc, tok: tok)
    self.fetching_stored_once = OnlyOnce(lambda s, loc, tok: tok)
    self.time_now = None
    self.facet_map = facet_map or {}
    self._parser = self._build_parser()

  def parse(self, bql_stmt):
    tokens = None
    try:
      self.time_now = int(time.time() * 1000)
      tokens = self._parser.parseString(bql_stmt, parseAll=True)
    except ParseException as err:
      raise err
    except ParseSyntaxException as err:
      raise err
    except ParseFatalException as err:
      raise err
    finally:
      self.reset_all()
    return tokens

  def accumulate_range_pred(self, field_map, pred):
    """Try to merge ANDed range predicates.
  
    For example, "year > 1999 AND year <= 2003" can be accumulated into
    "1999 < year <= 2003".
    """
  
    def _max(n1, include1, n2, include2):
      """Find the larger of the two lower bounds."""
  
      if n1 == None:
        return n2, include2
      elif n2 == None:
        return n1, include1
      else:
        if n1 > n2:
          return n1, include1
        elif n1 == n2:
          return n1, (include1 and include2)
        else:
          return n2, include2
  
    def _min(n1, include1, n2, include2):
      """Find the smaller of the two upper bounds."""
  
      if n1 == None:
        return n2, include2
      elif n2 == None:
        return n1, include1
      else:
        if n1 > n2:
          return n2, include2
        elif n1 == n2:
          return n1, (include1 and include2)
        else:
          return n1, include1
  
    field = pred_field(pred)
    old_range = field_map.get(field)
    if not old_range:
      field_map[field] = pred
      return
    old_spec = old_range.values()[0].values()[0]
    old_from = old_spec.get("from")
    old_include_lower = old_spec.get("include_lower") or False
    old_to = old_spec.get("to")
    old_include_upper = old_spec.get("include_upper") or False
  
    cur_spec = pred.values()[0].values()[0]
    cur_from = cur_spec.get("from")
    cur_include_lower = cur_spec.get("include_lower") or False
    cur_to = cur_spec.get("to")
    cur_include_upper = cur_spec.get("include_upper") or False
  
    new_spec = {}
    lower, include_lower = _max(old_from, old_include_lower, cur_from, cur_include_lower)
    upper, include_upper = _min(old_to, old_include_upper, cur_to, cur_include_upper)
  
    if lower and upper:
      if (lower > upper or
          (lower == upper and (not include_lower or not include_upper))):
        raise ParseSyntaxException(ParseException("", 0, "Conflict range predicates for column '%s'"
                                                  % field))
    if lower:
      new_spec["from"] = lower
      new_spec["include_lower"] = include_lower
    if upper:
      new_spec["to"] = upper
      new_spec["include_upper"] = include_upper
    field_map[field] = {"range": {field: new_spec} }

  def order_by_action(self, s, loc, tok):
    for order in tok[1:]:
      if (order[0] == PARAM_SORT_SCORE and len(order) > 1):
        raise ParseSyntaxException(ParseException(s, loc, '"ORDER BY %s" should not be followed by %s'
                                                  % (PARAM_SORT_SCORE, order[1])))

  def and_predicate_action(self, s, loc, tok):
    # print ">>> in and_predicate_action: tok = ", tok
    preds = []
    field_map = {}
    for i in xrange(0, len(tok[0]), 2):
      pred = tok[0][i]
      if pred_type(pred) != "range":
        preds.append(pred)
      else:
        self.accumulate_range_pred(field_map, pred)
    for f in field_map.values():
      preds.append(f)
    return {"and": preds}
  
  def or_predicate_action(self, s, loc, tok):
    # print ">>> in or_predicate_action: tok = ", tok
    preds = []
    for i in xrange(0, len(tok[0]), 2):
      if type(tok[0][i]) != dict:
        preds = tok[0][i].asList()
        preds.append(preds[0])
      else:
        preds.append(tok[0][i])
    return {"or": preds}
  
  def prop_list_action(self, s, loc, tok):
    # print ">>> in prop_list_action: tok = ", tok
    props = {}
    for i in xrange(0, len(tok), 2):
      props[tok[i]] = tok[i+1]
    return props
  
  def in_predicate_action(self, s, loc, tok):
    field = tok[0]

    facet_info = self.facet_map.get(field)
    facet_type = None
    pred = None
    if facet_info:
      facet_type = facet_info.get_props()["type"]
    if facet_type == "range":
      raise ParseSyntaxException(ParseException(
          s, loc, 'Column "%s" is a range facet (cannot be in an IN-predicate)'))

    ok, msg = self._verify_field_data_type(field, tok.value_list)
    if not ok:
      raise ParseSyntaxException(ParseException(s, loc, msg))
    ok, msg = self._verify_field_data_type(field, tok.except_values)
    if not ok:
      raise ParseSyntaxException(ParseException(s, loc, msg))

    if tok[1] != "not":
      return {"terms":
                {field:
                   {JSON_PARAM_VALUES: (tok.value_list[:] or []),
                    JSON_PARAM_EXCLUDES: (tok.except_values[:] or []),
                    JSON_PARAM_OPERATOR: PARAM_SELECT_OP_OR,
                    JSON_PARAM_NO_OPTIMIZE: False
                    }
                 }
              }
    else:
      return {"terms":
                {field:
                   {JSON_PARAM_VALUES: (tok.except_values[:] or []),
                    JSON_PARAM_EXCLUDES: (tok.value_list[:] or []),
                    JSON_PARAM_OPERATOR: PARAM_SELECT_OP_OR,
                    JSON_PARAM_NO_OPTIMIZE: False
                    }
                 }
              }
  
  def contains_all_predicate_action(self, s, loc, tok):
    field = tok[0]
    ok, msg = self._verify_field_data_type(field, tok.value_list)
    if not ok:
      raise ParseSyntaxException(ParseException(s, loc, msg))
    ok, msg = self._verify_field_data_type(field, tok.except_values)
    if not ok:
      raise ParseSyntaxException(ParseException(s, loc, msg))

    return {"terms":
              {field:
                 {JSON_PARAM_VALUES: (tok.value_list[:] or []),
                  JSON_PARAM_EXCLUDES: (tok.except_values[:] or []),
                  JSON_PARAM_OPERATOR: PARAM_SELECT_OP_AND,
                  # XXX Need to check this based on facet info
                  JSON_PARAM_NO_OPTIMIZE: False
                  }
               }
            }
  
  def query_predicate_action(self, s, loc, tok):
    return {JSON_PARAM_QUERY:
              {JSON_PARAM_QUERY_STRING:
                 {JSON_PARAM_QUERY: tok[2]}
               }
            }
  
  def equal_predicate_action(self, s, loc, tok):
    field = tok[0]
    value = tok[2]
    ok, msg = self._verify_field_data_type(field, [value])
    if not ok:
      raise ParseSyntaxException(ParseException(s, loc, msg))

    facet_info = self.facet_map.get(field)
    facet_type = None
    if facet_info:
      facet_type = facet_info.get_props()["type"]

    if facet_type == "range":
      return {"range":
                {field:
                   {"from": value,
                    "to": value,
                    "include_lower": True,
                    "include_upper": True,
                    }
                 }
              }
    elif facet_type == "path":
      path_spec = {"value": value}
      if tok.prop_list:
        for k,v in tok.prop_list.iteritems():
          if k in ["strict", "depth"]:
            path_spec[k] = v
          else:
            raise ParseSyntaxException(ParseException(
                s, loc, 'Property, "%s", is not supported for facet "%s"' % (k, field)))
      return {"path":
                {field: path_spec}
              }
    else:
      return {"term":
                {field:
                   {"value": value}
                 }
              }
  
  def not_equal_predicate_action(self, s, loc, tok):
    field = tok[0]
    value = tok[2]
    ok, msg = self._verify_field_data_type(field, [value])
    if not ok:
      raise ParseSyntaxException(ParseException(s, loc, msg))
    return {"terms":
              {field:
                 {JSON_PARAM_VALUES: [],
                  JSON_PARAM_EXCLUDES: [value],
                  JSON_PARAM_OPERATOR: PARAM_SELECT_OP_OR,
                  JSON_PARAM_NO_OPTIMIZE: False
                  }
               }
            }
  
  def range_predicate_action(self, s, loc, tok):
    # print ">>> in range_predicate_action: tok = ", tok
    field = tok[0]
    value = tok[2]
    ok, msg = self._verify_facet_type(field, "range")
    if not ok:
      raise ParseSyntaxException(ParseException(s, loc, msg))
    ok, msg = self._verify_field_data_type(field, [value])
    if not ok:
      raise ParseSyntaxException(ParseException(s, loc, msg))

    if tok[1] == ">" or tok[1] == ">=":
      return {"range":
                {field:
                   {"from": value,
                    "include_lower": tok[1] == ">="
                    }
                 }
              }
    else:
      return {"range":
                {tok[0]:
                   {"to": value,
                    "include_upper": tok[1] == "<="
                    }
                 }
              }
  
  def between_predicate_action(self, s, loc, tok):
    # print ">>> in between_predicate_action: tok = ", tok
    field = tok[0]
    ok, msg = self._verify_facet_type(field, "range")
    if not ok:
      raise ParseSyntaxException(ParseException(s, loc, msg))

    if tok[1] == "not":
      # "column NOT BETWEEN x AND y"
      from_value = tok[3]
      to_value = tok[5]
      ok, msg = self._verify_field_data_type(field, [from_value, to_value])
      if not ok:
        raise ParseSyntaxException(ParseException(s, loc, msg))
      return {"or": [{"range":
                        {tok[0]:
                           {"to": from_value,
                            "include_upper": False
                            }
                         }
                      },
                     {"range":
                        {tok[0]:
                           {"from": to_value,
                            "include_lower": False
                            }
                         }
                      }
                     ]
              }
    else:
      from_value = tok[2]
      to_value = tok[4]
      ok, msg = self._verify_field_data_type(field, [from_value, to_value])
      if not ok:
        raise ParseSyntaxException(ParseException(s, loc, msg))
      return {"range":
                {tok[0]:
                   {"from": from_value,
                    "to": to_value,
                    "include_lower": True,
                    "include_upper": True
                    }
                 }
              }
  
  def time_in_last_action(self, s, loc, tok):
    field = tok[0]
    ok, msg = self._verify_facet_type(field, "range")
    if not ok:
      raise ParseSyntaxException(ParseException(s, loc, msg))
    return {"range":
              {field:
                 {"from": tok[3],
                  "include_lower": False
                  }
               }
            }

  def time_since_action(self, s, loc, tok):
    field = tok[0]
    ok, msg = self._verify_facet_type(field, "range")
    if not ok:
      raise ParseSyntaxException(ParseException(s, loc, msg))
    if tok[1] in ["since", "after"]:
      return {"range":
                {field:
                   {"from": tok[2],
                    "include_lower": False
                    }
                 }
              }
    elif tok[1] in ["before"]:
      return {"range":
                {field:
                   {"to": tok[2],
                    "include_upper": False
                    }
                 }
              }

  def match_predicate_action(self, s, loc, tok):
    # print ">>> in match_predicate_action: tok = ", tok
    return {JSON_PARAM_QUERY:
              {JSON_PARAM_QUERY_STRING:
                 {"fields": tok[1][:],
                  JSON_PARAM_QUERY: tok[3]
                  }
               }
            }
  
  def like_predicate_action(self, s, loc, tok):
    field = tok[0]
    if self.facet_map.has_key(field):
      facet_info = self.facet_map[field]
      column_type = facet_info.get_props()["column_type"]
      if column_type != "string":
        raise ParseSyntaxException(
          ParseException(s, loc, 'Column, "%s", is not a string type' % field))
    # Convert % and _ in SQL syntax to Lucene's * and .
    value = tok[2].replace("%", "*").replace("_", "?")
    return {"query":
              {"wildcard":
                 {field: value}
               }
            }
  
  def reset_all(self):
    self.limit_once.reset()
    self.order_by_once.reset()
    self.group_by_once.reset()
    self.browse_by_once.reset()
    self.fetching_stored_once.reset()
  
  def convert_time(self, s, loc, toks):
    """Convert a time expression into an epoch time."""
  
    if toks[0] == "now":
      return self.time_now
    elif toks.date_time_regex:
      mm = DATE_TIME_REGEX.match(toks[0])
      (_, year, _, month, day, hour, minute, second) = mm.groups()
      try:
        time_stamp = datetime.strptime("%s-%s-%s %s:%s:%s" % (year, month, day, hour, minute, second),
                                       "%Y-%m-%d %H:%M:%S")
      except ValueError as err:
        raise ParseSyntaxException(ParseException(s, loc, "Invalid date/time string: %s" % toks[0]))
      return int(time.mktime(time_stamp.timetuple()) * 1000)
  
  def convert_time_span(self, s, loc, toks):
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
    
    return self.time_now - total

  def _verify_value_type(self, value, column_type):
    """Verify value type."""

    if column_type in ["int", "short"]:
      if type(value) == float:
        return False, 'Value, %s, is not of type "%s"' % (value, column_type)
      elif type(value) == str:
        return False, 'Value, "%s", is not of type "%s"' % (value, column_type)
      elif type(value) == bool:
        return False, 'Value, %s, is not of type "%s"' % (value and "true" or "false", column_type)
      else:
        return True, None
    elif column_type in ["float", "double"]:
      if type(value) == str:
        return False, 'Value, "%s", is not of type "%s"' % (value, column_type)
      elif type(value) == bool:
        return False, 'Value, %s, is not of type "%s"' % (value and "true" or "false", column_type)
      else:
        return True, None
    elif column_type in ["string"]:
      if type(value) in [int, float]:
        return False, 'Value, %s, is not of type "%s"' % (value, column_type)
      elif type(value) == bool:
        return False, 'Value, %s, is not of type "%s"' % (value and "true" or "false", column_type)
      else:
        return True, None
    elif column_type in ["boolean"]:
      # XXX Need to test this
      if value not in [True, False]:
        return False, 'Value, %s, is not of type "%s"' % (value, column_type)
      else:
        return True, None

  def _verify_field_data_type(self, field, values):
    """Validate data type for a list of values for a given field."""
    
    if self.facet_map.has_key(field):
      facet_info = self.facet_map[field]
      column_type = facet_info.get_props()["column_type"]
      for value in values:
        ok, msg = self._verify_value_type(value, column_type)
        if not ok:
          return ok, msg + ' (for facet "%s")' % field
    return True, None

  def _verify_facet_type(self, field, expected_type):
    """Validate facet type given a field."""

    facet_info = self.facet_map.get(field)
    if facet_info and facet_info.get_props()["type"] != expected_type:
      return False, 'Column, "%s", is not %s facet' % (field, expected_type)
    else:
      return True, None
    
  def _build_parser(self):
    """Build a BQL parser.
    """
    #
    # BQL tokens
    #
    # Remember to use lower case in the definition because we use
    # Keyword.match to do comparison at other places in the code.
    #
    ALL = Keyword("all", caseless=True)
    AFTER = Keyword("after", caseless=True)
    AGAINST = Keyword("against", caseless=True)
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
    LIKE = Keyword("like", caseless=True)
    LIMIT = Keyword("limit", caseless=True)
    LONG = Keyword("long", caseless=True)
    MATCH = Keyword("match", caseless=True)
    NOT = Keyword("not", caseless=True)
    NOW = Keyword("now", caseless=True)
    OR = Keyword("or", caseless=True)
    ORDER = Keyword("order", caseless=True)
    PARAM = Keyword("param", caseless=True)
    QUERY = Keyword("query", caseless=True)
    SELECT = Keyword("select", caseless=True)
    SET = Keyword("set", caseless=True)
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
    
    ident = Word(alphas + "_", alphanums + "_-.$")
    column_name = ~keyword + Word(alphas + "_", alphanums + "_-.")
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
                 Optional(time_millisecond_part)).setParseAction(self.convert_time_span)
    
    date_time_string = Regex(DATE_TIME).setResultsName("date_time_regex").setParseAction(self.convert_time)
    
    time_expr = ((time_span + AGO)
                 | date_time_string
                 | NOW.setParseAction(self.convert_time))
    
    number = (real | integer)       # Put real before integer to avoid ambiguity
    numeric = (time_expr | number)
    
    boolean_constant = (TRUE | FALSE).setParseAction(lambda t: t[0] == "true")
    
    value = (numeric | quotedString | boolean_constant)
    value_list = LPAR + delimitedList(value) + RPAR
    
    prop_pair = (quotedString + COLON + value)
    predicate_props = (WITH + LPAR + delimitedList(prop_pair).
                       setResultsName("prop_list").setParseAction(self.prop_list_action) + RPAR)
    
    in_predicate = (column_name + Optional(NOT) +
                    IN + value_list.setResultsName("value_list") +
                    Optional(EXCEPT + value_list.setResultsName("except_values")) +
                    Optional(predicate_props)
                    ).setResultsName("in_pred").setParseAction(self.in_predicate_action)
    
    contains_all_predicate = (column_name +
                              CONTAINS + ALL + value_list.setResultsName("value_list") +
                              Optional(EXCEPT + value_list.setResultsName("except_values")) +
                              Optional(predicate_props)
                              ).setResultsName("contains_all_pred").setParseAction(self.contains_all_predicate_action)
    
    equal_predicate = (column_name +
                       EQUAL + value +
                       Optional(predicate_props)
                       ).setResultsName("equal_pred").setParseAction(self.equal_predicate_action)
    
    not_equal_predicate = (column_name +
                           NOT_EQUAL + value +
                           Optional(predicate_props)
                           ).setResultsName("not_equal_pred").setParseAction(self.not_equal_predicate_action)
    
    query_predicate = (QUERY + IS + quotedString
                       ).setResultsName("query_pred").setParseAction(self.query_predicate_action)
    
    between_predicate = (column_name + Optional(NOT) +
                         BETWEEN + value + AND + value
                         ).setResultsName("between_pred").setParseAction(self.between_predicate_action)
    
    range_op = oneOf("< <= >= >")
    range_predicate = (column_name + range_op + value
                       ).setResultsName("range_pred").setParseAction(self.range_predicate_action)
    
    time_predicate = ((column_name + IN + LAST + time_span).setParseAction(self.time_in_last_action)
                      | (column_name + (SINCE | AFTER | BEFORE) + time_expr).setParseAction(self.time_since_action)
                      ).setResultsName("time_pred")
    
    match_predicate = (MATCH + LPAR + column_name_list + RPAR +
                       AGAINST + LPAR + quotedString + RPAR
                       ).setResultsName("match_pred").setParseAction(self.match_predicate_action)

    like_predicate = (column_name + LIKE + quotedString
                      ).setResultsName("like_pred").setParseAction(self.like_predicate_action)

    predicate = (in_predicate
                 | contains_all_predicate
                 | equal_predicate
                 | not_equal_predicate
                 | query_predicate
                 | between_predicate
                 | range_predicate
                 | time_predicate
                 | match_predicate
                 | like_predicate
                 )
    
    predicates = predicate + NotAny(OR) + ZeroOrMore(AND + predicate)
    
    search_expr = operatorPrecedence(predicate,
                                     [(AND, 2, opAssoc.LEFT, self.and_predicate_action),
                                      (OR,  2, opAssoc.LEFT, self.or_predicate_action)
                                      ])
    
    param_type = BOOLEAN | INT | LONG | STRING | BYTEARRAY | DOUBLE
    facet_param = Group(LPAR + facet_name + COMMA + quotedString + COMMA +
                        param_type + COMMA + value + RPAR).setResultsName("facet_param", listAllMatches=True)
    given_clause = (GIVEN + FACET + PARAM + delimitedList(facet_param))
    
    orderseq = ASC | DESC
    
    order_by_expression = Forward()
    order_by_spec = Group(column_name + Optional(orderseq)).setResultsName("orderby_spec", listAllMatches=True)
    order_by_expression << (order_by_spec + ZeroOrMore(COMMA + order_by_expression))
    order_by_clause = (ORDER + BY + order_by_expression).setResultsName("orderby").setParseAction(self.order_by_once)
    
    limit_clause = (LIMIT + Group(Optional(integer + COMMA) + integer)).setResultsName("limit").setParseAction(self.limit_once)
    
    expand_flag = TRUE | FALSE
    facet_order_by = HITS | VALUE
    facet_spec = Group(column_name +
                       Optional(LPAR + expand_flag + COMMA + integer  + COMMA + integer + COMMA + facet_order_by + RPAR))
    
    group_by_clause = (GROUP + BY +
                       column_name.setResultsName("groupby") +
                       Optional(TOP + integer.setResultsName("max_per_group"))).setParseAction(self.group_by_once)
    
    browse_by_clause = (BROWSE + BY +
                        delimitedList(facet_spec).setResultsName("facet_specs")).setParseAction(self.browse_by_once)
    
    fetching_flag = TRUE | FALSE
    fetching_stored_clause = (FETCHING + STORED +
                              Optional(fetching_flag)).setResultsName("fetching_stored").setParseAction(self.fetching_stored_once)
    
    additional_clause = (order_by_clause
                         | limit_clause
                         | group_by_clause
                         | browse_by_clause
                         | fetching_stored_clause
                         )
    
    additional_clauses = ZeroOrMore(additional_clause)
    
    from_clause = (FROM + ident.setResultsName("index"))
    
    select_stmt << (SELECT + 
                    ('*' | column_name_list).setResultsName("columns") + 
                    Optional(from_clause) +
                    Optional(WHERE + search_expr.setResultsName("where")) +
                    Optional(given_clause.setResultsName("given")) +
                    additional_clauses
                    )
    
    describe_stmt = (DESC | DESCRIBE).setResultsName("describe") + Optional(ident.setResultsName("index"))

    set_stmt = (SET +
                ident.setResultsName("variable") +
                (value.setResultsName("value") | value_list.setResultsName("value_list"))
                )

    BQLstmt = (select_stmt
               | describe_stmt
               | set_stmt
               ) + Optional(SEMICOLON) + stringEnd
    
    # Define comment format, and ignore them
    sql_comment = "--" + restOfLine
    BQLstmt.ignore(sql_comment)
    return BQLstmt

# End of class BQLParser


#
# Some functions that will be shared by BQL Parser and BQLRequest, etc.
#

def pred_type(pred):
  return pred.keys()[0]

def pred_field(pred):
  return pred.values()[0].keys()[0]

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
  def _max(n1, n2):
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

  def _min(n1, n2):
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

  low = _max(low1, low2)
  high = _min(high1, high2)

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


class BQLRequest:
  """A Sensei request with a BQL statement.

  The BQL statement can be one of the following statements:

  1. SELECT
  2. DESCRIBE

  """

  def __init__(self, tokens, facet_map):
    self.tokens = tokens
    self.facet_map = facet_map
    self.query = ""
    self.selections = None
    self.selection_list = []
    self.filter = None
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
      assert type(where) == dict
      self._extract_query_filter_selections(where)

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

  def _extract_query_filter_selections(self, where):
    """Extract the query and filter information from the where clause."""

    filter_list = []
    if where.get(JSON_PARAM_QUERY):
      self.query_pred = where
      self.filter = None
    elif where.get("and"):
      preds = where.get("and")
      for pred in preds:
        if pred.get(JSON_PARAM_QUERY):
          # If there is no query yet, use predicate as a query; otherwise,
          # treat this predicate as a regular filter.
          if not self.query_pred:
            self.query_pred = pred
          else:
            filter_list.append(pred)
        elif pred.get("or") or pred.get("and") or pred.get("bool"):
          # XXX Need to clear this part
          filter_list.append(pred)
        elif self._is_facet(pred_field(pred)):
          self.selection_list.append(pred)
        else:
          filter_list.append(pred)
      if len(filter_list) == 1:
        self.filter = filter_list[0]
      elif filter_list:
        self.filter = {"and": filter_list}
    elif where.get("or"):
      self.filter = where
    elif self._is_facet(pred_field(where)):
      self.selection_list.append(where)
    elif where:
      self.filter = where

    # XXX Do merging, etc. on self.selection_list
    self.selections = self.selection_list

  def _is_facet(self, pred_field):
    """Check if a field is a facet."""

    return self.facet_map.has_key(pred_field)

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

    # TODO finish the implementation
    self.selections = self.selection_list

  # def merge_selections_old(self):
  #   """Merge all selections and detect conflicts."""
  # 
  #   self.selections = {}
  #   for selection in self.selection_list:
  #     existing = self.selections.get(selection.field)
  #     if existing:
  #       # Try to merge simple range predicates
  #       if (len(selection.getValues()) == 1 and
  #           selection.getType() == SELECTION_TYPE_RANGE and
  #           existing.getType() == SELECTION_TYPE_RANGE):
  #         new_values = and_range_list(existing.getValues(), selection.getValues()[0])
  #         if not new_values:
  #           raise SenseiClientError("There is conflict in selection(s) for column '%s'" % selection.field)
  #         existing.setValues(new_values)
  #       else:
  #         # Don't bother trying to merge predicates
  #         if existing.getValues() and selection.getValues():
  #           return False, "There is conflict in selection(s) for column '%s'" % selection.field
  #         if selection.getValues():
  #           existing.setValues(selection.getValues())
  #         if selection.getExcludes():
  #           existing.setExcludes(merge_values(existing.getExcludes(),
  #                                           selection.getExcludes()))
  #       # XXX How about props?
  #     else:
  #       self.selections[selection.field] = selection
  #   return True, None

  def get_selections(self):
    """Get all the selections from in statement."""

    # if self.selections == None:
    #   self.merge_selections()
    return self.selections

  def get_filter(self):
    """Get the filter from the statement."""

    return self.filter

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
  return


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

> select color,category, tags from cars where color like "bl%" and match(contents) against ("cool AND moon-roof") and category like "%an"
+-------+----------+---------------------------+
| color | category | tags                      |
+-------+----------+---------------------------+
| blue  | sedan    | cool,moon-roof,reliable   |
| blue  | sedan    | cool,moon-roof,navigation |
| black | sedan    | cool,moon-roof,reliable   |
| blue  | van      | cool,moon-roof,reliable   |
| blue  | sedan    | cool,moon-roof,navigation |
| blue  | sedan    | cool,moon-roof,reliable   |
| blue  | sedan    | cool,moon-roof,reliable   |
| blue  | sedan    | cool,moon-roof,reliable   |
| black | sedan    | cool,moon-roof,reliable   |
| blue  | sedan    | cool,moon-roof,reliable   |
+-------+----------+---------------------------+
10 rows in set, 51 hits, 15001 total docs (server: 17ms, total: 450ms)


"""
