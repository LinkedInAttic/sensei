package com.senseidb.gateway.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Comparator;
import java.util.Set;

import javax.naming.ConfigurationException;

import org.json.JSONObject;

import com.linkedin.zoie.api.DataConsumer.DataEvent;
import com.linkedin.zoie.dataprovider.jdbc.JDBCConnectionFactory;
import com.linkedin.zoie.dataprovider.jdbc.JDBCStreamDataProvider;
import com.linkedin.zoie.dataprovider.jdbc.PreparedStatementBuilder;
import com.linkedin.zoie.impl.indexing.StreamDataProvider;
import com.linkedin.zoie.impl.indexing.ZoieConfig;

import com.senseidb.gateway.SenseiGateway;
import com.senseidb.indexing.DataSourceFilter;
import com.senseidb.indexing.ShardingStrategy;

public class JdbcDataProviderBuilder extends SenseiGateway<ResultSet>{

	private Comparator<String> _versionComparator;

	@Override
	public void start() {
	  _versionComparator = pluginRegistry.getBeanByName("versionComparator", Comparator.class);
	  if (_versionComparator == null) _versionComparator = ZoieConfig.DEFAULT_VERSION_COMPARATOR;
	}


  @Override
	public StreamDataProvider<JSONObject> buildDataProvider(final DataSourceFilter<ResultSet> dataFilter,
      String oldSinceKey,
      ShardingStrategy shardingStrategy,
      Set<Integer> partitions) throws Exception
  {

	       final String url = config.get("jdbc.url");
	       final String username = config.get("jdbc.username");
	       final String password = config.get("jdbc.password");
	       final String driver = config.get("jdbc.driver");
         final String adaptor = config.get("jdbc.adaptor");

         final SenseiJDBCAdaptor senseiAdaptor =
           pluginRegistry.getBeanByName(adaptor, SenseiJDBCAdaptor.class) != null ?
           pluginRegistry.getBeanByName(adaptor, SenseiJDBCAdaptor.class) :
           pluginRegistry.getBeanByFullPrefix(adaptor, SenseiJDBCAdaptor.class);

	       if (senseiAdaptor==null){
           throw new ConfigurationException("adaptor not found: " + adaptor);
	       }


		   JDBCConnectionFactory connFactory = new JDBCConnectionFactory() {


			 private Connection _conn = null;

			 @Override
			 public void showndown() throws SQLException {
				 if (_conn!=null){
					_conn.close();
				 }
			 }

			 @Override
			 public Connection getConnection() throws SQLException {
				if (_conn == null){
			 	  try {
					Class.forName (driver).newInstance ();
				  } catch (Exception e) {
					throw new SQLException("unable to load driver: "+e.getMessage());
				  }
		          _conn = DriverManager.getConnection (url, username, password);
				}
				return _conn;
			 }
		    };

		    PreparedStatementBuilder<JSONObject> stmtBuilder = new PreparedStatementBuilder<JSONObject>() {

		    	private final DataSourceFilter<ResultSet> filter = dataFilter;
				@Override
				public PreparedStatement buildStatment(Connection conn,
						String fromVersion) throws SQLException {
					return senseiAdaptor.buildStatment(conn, fromVersion);
				}

				@Override
				public DataEvent<JSONObject> buildDataEvent(ResultSet rs)
						throws SQLException {
					try{
					  JSONObject jsonObject = filter.filter(rs);
					  return new DataEvent<JSONObject>(jsonObject, senseiAdaptor.extractVersion(rs));
					}
					catch(Exception e){
						throw new SQLException(e.getMessage(),e);
					}
				}
			};

	    return new JDBCStreamDataProvider<JSONObject>(connFactory, stmtBuilder, _versionComparator);
	}


  @Override
  public Comparator<String> getVersionComparator() {
    return _versionComparator;
  }

}
