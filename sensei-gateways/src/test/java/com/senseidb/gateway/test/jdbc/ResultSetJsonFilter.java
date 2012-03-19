package com.senseidb.gateway.test.jdbc;

import java.sql.ResultSet;

import org.json.JSONObject;

import com.senseidb.indexing.DataSourceFilter;

public class ResultSetJsonFilter extends DataSourceFilter<ResultSet> {

  @Override
  protected JSONObject doFilter(ResultSet rs) throws Exception {
    String json = rs.getString(1);
    return new JSONObject(json);
  }

}
