package com.sensei.search.client.servlet;

public interface SenseiSearchServletParams {
	  public static final String PARAM_OFFSET = "start";
	  public static final String PARAM_COUNT = "rows";
	  public static final String PARAM_QUERY = "q";
	  public static final String PARAM_SORT = "sort";
	  public static final String PARAM_FETCH_STORED = "fetchstored";
	  public static final String PARAM_SHOW_EXPLAIN = "showexplain";

	  public static final String PARAM_RESULT_HIT_EXPLANATION = "explanation";
	  public static final String PARAM_RESULT_FACETS = "facets";
	  
	  public static final String PARAM_CLUSTER_NAME = "cluster.name";
	  public static final String PARAM_ZOOKEEPER_URL = "cluster.zookeeper.url";
	  public static final String PARAM_ZOOKEEPER_TIMEOUT = "cluster.zookeeper.conn.timeout";
	  
	  public static final String PARAM_RESULT_TOTALDOCS = "totaldocs";
	  public static final String PARAM_RESULT_NUMHITS = "numhits";
	  public static final String PARAM_RESULT_HITS = "hits";
	  public static final String PARAM_RESULT_HIT_UID = "uid";
	  public static final String PARAM_RESULT_HIT_SCORE = "score";
	  public static final String PARAM_RESULT_TIME = "time";
}
