package com.sensei.indexing.api.jdbc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public interface SenseiJDBCAdaptor {
  PreparedStatement buildStatment(Connection conn,String fromVersion) throws SQLException;
  String extractVersion(ResultSet resultSet) throws SQLException;
}
