package com.sensei.search.client.servlet;

public interface SenseiSearchServletParams {
	  public static final String PARAM_OFFSET = "start";
	  public static final String PARAM_COUNT = "rows";
	  public static final String PARAM_QUERY = "q";
	  public static final String PARAM_QUERY_PARAM = "qparam";
	  public static final String PARAM_SORT = "sort";
	  public static final String PARAM_SORT_ASC = "asc";
	  public static final String PARAM_SORT_DESC = "desc";
	  public static final String PARAM_SORT_SCORE = "relevance";
	  public static final String PARAM_FETCH_STORED = "fetchstored";
	  public static final String PARAM_SHOW_EXPLAIN = "showexplain";
	  public static final String PARAM_SELECT = "select";
	  public static final String PARAM_SELECT_VAL = "val";
	  public static final String PARAM_SELECT_NOT = "not";
	  public static final String PARAM_SELECT_OP = "op";
	  public static final String PARAM_SELECT_OP_AND = "and";
	  public static final String PARAM_SELECT_OP_OR = "or";
	  public static final String PARAM_SELECT_PROP = "prop";
    public static final String PARAM_FACET = "facet";
    public static final String PARAM_DYNAMIC_INIT = "dyn";

	  public static final String PARAM_FACET_EXPAND = "expand";
	  public static final String PARAM_FACET_MAX = "max";
	  public static final String PARAM_FACET_MINHIT = "minhit";
	  public static final String PARAM_FACET_ORDER = "order";
	  public static final String PARAM_FACET_ORDER_HITS = "hits";
	  public static final String PARAM_FACET_ORDER_VAL = "val";

    public static final String PARAM_DYNAMIC_TYPE = "type";
    public static final String PARAM_DYNAMIC_VAL = "vals";
    public static final String PARAM_DYNAMIC_VAL_DELIM = ";";

	  public static final String PARAM_RESULT_PARSEDQUERY = "parsedquery";
	  public static final String PARAM_RESULT_HIT_EXPLANATION = "explanation";
	  public static final String PARAM_RESULT_FACETS = "facets";
	  
	  public static final String PARAM_RESULT_TOTALDOCS = "totaldocs";
	  public static final String PARAM_RESULT_NUMHITS = "numhits";
	  public static final String PARAM_RESULT_HITS = "hits";
	  public static final String PARAM_RESULT_HIT_UID = "uid";
	  public static final String PARAM_RESULT_HIT_SCORE = "score";
	  public static final String PARAM_RESULT_TIME = "time";
}
