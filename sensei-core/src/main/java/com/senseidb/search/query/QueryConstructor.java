package com.senseidb.search.query;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.Query;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.senseidb.search.query.ScoreAugmentQuery.ScoreAugmentFunction;
import com.senseidb.search.relevance.RelevanceFunctionBuilder;
import com.senseidb.search.relevance.impl.RelevanceJSONConstants;

public abstract class QueryConstructor
{
  public static final String VALUES_PARAM                       = "values";
  public static final String EXCLUDES_PARAM                     = "excludes";
  public static final String OPERATOR_PARAM                     = "operator";
  public static final String PARAMS_PARAM                       = "params";
  public static final String MUST_PARAM                         = "must";
  public static final String MUST_NOT_PARAM                     = "must_not";
  public static final String SHOULD_PARAM                       = "should";
  public static final String BOOST_PARAM                        = "boost";
  public static final String DISABLE_COORD_PARAM                = "disable_coord";
  public static final String MINIMUM_NUMBER_SHOULD_MATCH_PARAM  = "minimum_number_should_match";
  public static final String TYPE_PARAM                         = "type";
  public static final String PHRASE_PARAM                       = "phrase";
  public static final String PHRASE_PREFIX_PARAM                = "phrase_prefix";
  public static final String AND_PARAM                          = "and";
  public static final String INCLUDE_PARAM                      = "include";
  public static final String EXCLUDE_PARAM                      = "exclude";
  public static final String SPAN_TERM_PARAM                    = "span_term";
  public static final String MATCH_PARAM                        = "match";
  public static final String END_PARAM                          = "end";
  public static final String CLASS_PARAM                        = "class";
  public static final String MINIMUM_MATCH_PARAM                = "minimum_match";
  public static final String CLAUSES_PARAM                      = "clauses";
  public static final String SLOP_PARAM                         = "slop";
  public static final String IN_ORDER_PARAM                     = "in_order";
  public static final String COLLECT_PAYLOADS_PARAM             = "collect_payloads";
  public static final String VALUE_PARAM                        = "value";
  public static final String QUERY_PARAM                        = "query";
  public static final String FILTER_PARAM                       = "filter";
  public static final String QUERIES_PARAM                      = "queries";
  public static final String TERM_PARAM                         = "term";
  public static final String TIE_BREAKER_PARAM                  = "tie_breaker";
  public static final String DEFAULT_FIELD_PARAM                = "default_field";
  public static final String FIELDS_PARAM                       = "fields";
  public static final String DEFAULT_OPERATOR_PARAM             = "default_operator";
  public static final String ALLOW_LEADING_WILDCARD_PARAM       = "allow_leading_wildcard";
  public static final String LOWERCASE_EXPANDED_TERMS_PARAM     = "lowercase_expanded_terms";
  public static final String ENABLE_POSITION_INCREMENTS_PARAM   = "enable_position_increments";
  public static final String FUZZY_PREFIX_LENGTH_PARAM          = "fuzzy_prefix_length";
  public static final String FUZZY_MIN_SIM_PARAM                = "fuzzy_min_sim";
  public static final String PHRASE_SLOP_PARAM                  = "phrase_slop";
  public static final String ANALYZE_WILDCARD_PARAM             = "analyze_wildcard";
  public static final String AUTO_GENERATE_PHRASE_QUERIES_PARAM = "auto_generate_phrase_queries";
  public static final String USE_DIS_MAX_PARAM                  = "use_dis_max";
  public static final String RELEVANCE                          = "relevance";

  public static final String LEFT_VALUE                         = "lvalue";
  public static final String RIGHT_VALUE                        = "rvalue";
  public static final String OP_IN                              = "in";
  public static final String OP_NOT_IN                          = "not_in";
  public static final String OP_GT                              = ">";
  public static final String OP_GE                              = ">=";
  public static final String OP_EQUAL                           = "==";
  public static final String OP_NOT_EQUAL                       = "!=";
  public static final String OP_LT                              = "<";
  public static final String OP_LE                              = "<=";
  public static final String FUNCTION_NAME                      = "function";
  
  private static final Map<String,QueryConstructor> QUERY_CONSTRUCTOR_MAP = new HashMap<String,QueryConstructor>();

  static
  {
    QUERY_CONSTRUCTOR_MAP.put(DistMaxQueryConstructor.QUERY_TYPE, new DistMaxQueryConstructor());
    QUERY_CONSTRUCTOR_MAP.put(PrefixQueryConstructor.QUERY_TYPE, new PrefixQueryConstructor());
    QUERY_CONSTRUCTOR_MAP.put(WildcardQueryConstructor.QUERY_TYPE, new WildcardQueryConstructor());
    QUERY_CONSTRUCTOR_MAP.put(RangeQueryConstructor.QUERY_TYPE, new RangeQueryConstructor());
    QUERY_CONSTRUCTOR_MAP.put(PathQueryConstructor.QUERY_TYPE, new PathQueryConstructor());
    QUERY_CONSTRUCTOR_MAP.put(SpanFirstQueryConstructor.QUERY_TYPE, new SpanFirstQueryConstructor());
    QUERY_CONSTRUCTOR_MAP.put(SpanNearQueryConstructor.QUERY_TYPE, new SpanNearQueryConstructor());
    QUERY_CONSTRUCTOR_MAP.put(SpanNotQueryConstructor.QUERY_TYPE, new SpanNotQueryConstructor());
    QUERY_CONSTRUCTOR_MAP.put(SpanOrQueryConstructor.QUERY_TYPE, new SpanOrQueryConstructor());
    QUERY_CONSTRUCTOR_MAP.put(SpanTermQueryConstructor.QUERY_TYPE, new SpanTermQueryConstructor());
    QUERY_CONSTRUCTOR_MAP.put(MatchAllQueryConstructor.QUERY_TYPE, new MatchAllQueryConstructor());
    QUERY_CONSTRUCTOR_MAP.put(TermQueryConstructor.QUERY_TYPE, new TermQueryConstructor());
    QUERY_CONSTRUCTOR_MAP.put(TermsQueryConstructor.QUERY_TYPE, new TermsQueryConstructor());
    QUERY_CONSTRUCTOR_MAP.put(UIDQueryConstructor.QUERY_TYPE, new UIDQueryConstructor());
    QUERY_CONSTRUCTOR_MAP.put(CustomQueryConstructor.QUERY_TYPE, new CustomQueryConstructor());
    QUERY_CONSTRUCTOR_MAP.put(ConstExpQueryConstructor.QUERY_TYPE, new ConstExpQueryConstructor());
  }
  
  public static QueryConstructor getQueryConstructor(String type, QueryParser qparser)
  {
    QueryConstructor queryConstructor = QUERY_CONSTRUCTOR_MAP.get(type);
    if (queryConstructor == null)
    {
      if (QueryStringQueryConstructor.QUERY_TYPE.equals(type))
        queryConstructor = new QueryStringQueryConstructor(qparser);
      else if (TextQueryConstructor.QUERY_TYPE.equals(type))
        queryConstructor = new TextQueryConstructor(qparser);
      else if (BooleanQueryConstructor.QUERY_TYPE.equals(type))
        queryConstructor = new BooleanQueryConstructor(qparser);
      else if (FilteredQueryConstructor.QUERY_TYPE.equals(type))
        queryConstructor = new FilteredQueryConstructor(qparser);
    }
    return queryConstructor;
  }

  public static Query constructQuery(JSONObject jsonQuery, QueryParser qparser) throws JSONException
  {
    if (jsonQuery == null)
      return null;

    Iterator<String> iter = jsonQuery.keys();
    if (!iter.hasNext())
      return null;

    String type = iter.next();

    QueryConstructor queryConstructor = QueryConstructor.getQueryConstructor(type, qparser);
    if (queryConstructor == null)
      throw new IllegalArgumentException("Query type '" + type + "' not supported");

    JSONObject jsonValue = jsonQuery.getJSONObject(type);
    Query baseQuery = queryConstructor.doConstructQuery(jsonValue);
    
    JSONObject jsonRelevance = null;
    if(jsonValue.has(RELEVANCE))
    {
      jsonRelevance = jsonValue.optJSONObject(RELEVANCE);
      if(jsonRelevance == null)
        throw new JSONException("relevance part of the query json can not be parsed.");
    }
    if(jsonRelevance == null)
    {
      return baseQuery;
    }
    else
    {
        // the olde code path, now turned off;
//      return new RelevanceQuery(baseQuery, jsonRelevance);
      
      ScoreAugmentFunction func = RelevanceFunctionBuilder.build(jsonRelevance);
      JSONObject valuesJson = jsonRelevance.optJSONObject(RelevanceJSONConstants.KW_VALUES); 
      if(func == null)
        throw new JSONException("Can not create the score function;");
      
      return new ScoreAugmentQuery(baseQuery, func, valuesJson);
    }
  }

  abstract protected Query doConstructQuery(JSONObject jsonQuery) throws JSONException;
}
