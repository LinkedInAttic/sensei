package com.senseidb.gateway.test.jdbc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import com.senseidb.gateway.jdbc.SenseiJDBCAdaptor;

public class SimpleJDBCAdaptor implements SenseiJDBCAdaptor{

  private static String sql = "select json,version from test where version > ?";
  @Override
  public PreparedStatement buildStatment(Connection conn, String fromVersion)
      throws SQLException {
    PreparedStatement stmt = conn.prepareStatement(sql);
    
    int version = fromVersion == null ? 0 : Integer.parseInt(fromVersion);
    stmt.setInt(1, version);
    return stmt;
  }

  @Override
  public String extractVersion(ResultSet resultSet) throws SQLException {
    int version = resultSet.getInt(2);
    return String.valueOf(version);
  }
}
