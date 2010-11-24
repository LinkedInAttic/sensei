package com.sensei.search.client.servlet;

public interface SenseiSearchServletParams {
	 public static final String PARAM_OFFSET = "offset";
	  public static final String PARAM_COUNT = "count";
	  public static final String PARAM_QUERY = "q";
	  public static final String PARAM_USERIDS = "userids";
	  public static final String PARAM_FETCH_STORED = "fetchstored";
	  public static final String PARAM_SHOW_EXPLAIN = "showexplain";
	  
	  public static final String PARAM_CLUSTER_NAME = "cluster.name";
	  public static final String PARAM_ZOOKEEPER_URL = "cluster.zookeeper.url";
	  public static final String PARAM_ZOOKEEPER_TIMEOUT = "cluster.zookeeper.conn.timeout";
	  
	  public static final String PARAM_RESULT_TOTALDOCS = "totaldocs";
	  public static final String PARAM_RESULT_NUMHITS = "numhits";
	  public static final String PARAM_RESULT_HITS = "hits";
	  public static final String PARAM_RESULT_HIT_UID = "uid";
	  public static final String PARAM_RESULT_TIME = "time";
}
