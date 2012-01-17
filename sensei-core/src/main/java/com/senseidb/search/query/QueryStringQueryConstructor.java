package com.senseidb.search.query;

import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.DisjunctionMaxQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.Version;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

public class QueryStringQueryConstructor extends QueryConstructor {

  public static final String QUERY_TYPE = "query_string";

  private QueryParser _qparser;

  public QueryStringQueryConstructor(QueryParser qparser)
  {
    _qparser = qparser;
  }

  @Override
  protected Query doConstructQuery(JSONObject jsonQuery) throws JSONException
  {
    String queryText = jsonQuery.optString(QUERY_PARAM, null);
    if (queryText == null || queryText.length() == 0)
    {
      return new MatchAllDocsQuery();
    }
    try
    {
      for (String name : JSONObject.getNames(jsonQuery))
      {
        if (QUERY_PARAM.equals(name))
          continue;
        if (BOOST_PARAM.equals(name))
          continue;

        List<String> fields = new ArrayList<String>();
        String defaultField = jsonQuery.optString(DEFAULT_FIELD_PARAM, null);
        if (defaultField != null && defaultField.length() != 0)
          fields.add(defaultField);

        JSONArray fieldsArray = jsonQuery.optJSONArray(FIELDS_PARAM);
        if (fieldsArray != null)
        {
          for (int i=0; i<fieldsArray.length(); ++i)
          {
            String field = fieldsArray.optString(i, null);
            if (field != null && field.length() != 0)
              fields.add(field);
          }
        }

        if (fields.size() == 0)
          fields.add("contents");

        String default_operator = jsonQuery.optString(DEFAULT_OPERATOR_PARAM, "or");

        boolean allow_leading_wildcard = jsonQuery.optBoolean(ALLOW_LEADING_WILDCARD_PARAM, true);
        boolean lowercase_expanded_terms = jsonQuery.optBoolean(LOWERCASE_EXPANDED_TERMS_PARAM, true);
        boolean enable_position_increments = jsonQuery.optBoolean(ENABLE_POSITION_INCREMENTS_PARAM, true);

        int fuzzy_prefix_length = jsonQuery.optInt(FUZZY_PREFIX_LENGTH_PARAM, 0);
        float fuzzy_min_sim = (float)jsonQuery.optDouble(FUZZY_MIN_SIM_PARAM, 0.5);
        int phrase_slop = jsonQuery.optInt(PHRASE_SLOP_PARAM, 0);

        //boolean analyze_wildcard = jsonQuery.optBoolean(ANALYZE_WILDCARD_PARAM, false);
        //boolean auto_generate_phrase_queries = jsonQuery.optBoolean(AUTO_GENERATE_PHRASE_QUERIES_PARAM, false);

        boolean use_dis_max = jsonQuery.optBoolean(USE_DIS_MAX_PARAM, true);
        float tie_breaker = (float)jsonQuery.optDouble(TIE_BREAKER_PARAM, .0);

        List<Query> queries = new ArrayList<Query>(fields.size());

        for (String field : fields)
        {
          QueryParser qparser = new QueryParser(Version.LUCENE_CURRENT, field, _qparser.getAnalyzer());
          qparser.setAllowLeadingWildcard(allow_leading_wildcard);
          qparser.setEnablePositionIncrements(enable_position_increments);
          qparser.setFuzzyMinSim(fuzzy_min_sim);
          qparser.setFuzzyPrefixLength(fuzzy_prefix_length);
          qparser.setLowercaseExpandedTerms(lowercase_expanded_terms);
          qparser.setPhraseSlop(phrase_slop);
          queries.add(qparser.parse(queryText));
        }

        if (fields.size() == 1)
        {
          Query query = queries.get(0);
          query.setBoost((float)jsonQuery.optDouble(BOOST_PARAM, 1.0));
          return query;
        }

        if (use_dis_max)
        {
          Query query = new DisjunctionMaxQuery(queries, tie_breaker);
          query.setBoost((float)jsonQuery.optDouble(BOOST_PARAM, 1.0));
          return query;
        }
        else
        {
          BooleanQuery query = new BooleanQuery();
          if (AND_PARAM.equals(default_operator))
          {
            for (Query q : queries)
            {
              query.add(q, BooleanClause.Occur.MUST);
            }
          }
          else
          {
            for (Query q : queries)
            {
              query.add(q, BooleanClause.Occur.SHOULD);
            }
          }
          query.setBoost((float)jsonQuery.optDouble(BOOST_PARAM, 1.0));
          return query;
        }
      }

      synchronized(_qparser)
      {
        Query query = _qparser.parse(queryText);
        query.setBoost((float)jsonQuery.optDouble(BOOST_PARAM, 1.0));
        return query;
      }
    }
    catch (ParseException e) {
      throw new JSONException(e);
    }
  }

}
