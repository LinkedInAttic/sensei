package com.sensei.indexing.api.gateway.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Comparator;

import javax.naming.ConfigurationException;

import org.apache.commons.configuration.Configuration;
import org.json.JSONObject;

import proj.zoie.api.DataConsumer.DataEvent;
import proj.zoie.dataprovider.jdbc.JDBCConnectionFactory;
import proj.zoie.dataprovider.jdbc.JDBCStreamDataProvider;
import proj.zoie.dataprovider.jdbc.PreparedStatementBuilder;
import proj.zoie.impl.indexing.StreamDataProvider;

import com.sensei.indexing.api.DataSourceFilter;
import com.sensei.indexing.api.gateway.SenseiGateway;
import com.sensei.plugin.SenseiPluginRegistry;

public class JdbcDataProviderBuilder extends SenseiGateway<ResultSet>{

	public static final String name = "jdbc";
	private SenseiPluginRegistry pluginRegistry;
	private Comparator<String> _versionComparator;

	public JdbcDataProviderBuilder(Configuration conf) throws Exception{
	  super(conf);
	   pluginRegistry = SenseiPluginRegistry.get(conf);
    _versionComparator = pluginRegistry.getBeanByName("versionComparator", Comparator.class);
	}

	@Override
	public StreamDataProvider<JSONObject> buildDataProvider(final DataSourceFilter<ResultSet> dataFilter,
			String oldSinceKey) throws Exception{

	       final String url = _conf.getString("url");
	       final String username = _conf.getString("username",null);
	       final String password = _conf.getString("password",null);
	       final String driver = _conf.getString("driver");
	       final String adaptor = _conf.getString("adaptor");

	       final SenseiJDBCAdaptor senseiAdaptor =  pluginRegistry.getBeanByFullPrefix("jdbc.adaptor", SenseiJDBCAdaptor.class);
	       if (senseiAdaptor==null){
	    	   throw new ConfigurationException("adaptor not found: "+adaptor);
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
