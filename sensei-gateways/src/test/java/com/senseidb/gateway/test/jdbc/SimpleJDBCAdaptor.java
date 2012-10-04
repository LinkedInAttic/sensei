/**
 * This software is licensed to you under the Apache License, Version 2.0 (the
 * "Apache License").
 *
 * LinkedIn's contributions are made under the Apache License. If you contribute
 * to the Software, the contributions will be deemed to have been made under the
 * Apache License, unless you expressly indicate otherwise. Please do not make any
 * contributions that would be inconsistent with the Apache License.
 *
 * You may obtain a copy of the Apache License at http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, this software
 * distributed under the Apache License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the Apache
 * License for the specific language governing permissions and limitations for the
 * software governed under the Apache License.
 *
 * © 2012 LinkedIn Corp. All Rights Reserved.  
 */

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
