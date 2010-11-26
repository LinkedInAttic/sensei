package com.sensei.search.client.servlet;

import static com.sensei.search.client.servlet.SenseiSearchServletParams.PARAM_CLUSTER_NAME;
import static com.sensei.search.client.servlet.SenseiSearchServletParams.PARAM_ZOOKEEPER_TIMEOUT;
import static com.sensei.search.client.servlet.SenseiSearchServletParams.PARAM_ZOOKEEPER_URL;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.Charset;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.log4j.Logger;

import com.linkedin.norbert.javacompat.cluster.ClusterClient;
import com.linkedin.norbert.javacompat.cluster.ZooKeeperClusterClient;
import com.linkedin.norbert.javacompat.network.NetworkClientConfig;
import com.sensei.search.cluster.client.SenseiNetworkClient;
import com.sensei.search.cluster.routing.UniformPartitionedRoutingFactory;
import com.sensei.search.nodes.SenseiBroker;
import com.sensei.search.nodes.impl.NoopRequestScatterRewriter;
import com.sensei.search.req.SenseiRequest;
import com.sensei.search.req.SenseiResult;

public abstract class AbstractSenseiClientServlet extends HttpServlet {

	private static final long serialVersionUID = 1L;
	
	private static final Logger logger = Logger.getLogger(AbstractSenseiClientServlet.class);

	private final UniformPartitionedRoutingFactory _routerFactory = new UniformPartitionedRoutingFactory();
	private final NoopRequestScatterRewriter _reqRewriter = new NoopRequestScatterRewriter();
	private final NetworkClientConfig _networkClientConfig = new NetworkClientConfig();
	
	private ClusterClient _clusterClient = null;
	private SenseiNetworkClient _networkClient = null;
	private SenseiBroker _senseiBroker = null;
	public AbstractSenseiClientServlet() {
	
	}

	@Override
	public void init(ServletConfig config) throws ServletException {
		super.init(config);
		String confFileName = config.getInitParameter("config.file");
		File confFile = new File(confFileName);
		try {
			PropertiesConfiguration conf = new PropertiesConfiguration(confFile);
			
			String zkurl = conf.getString(PARAM_ZOOKEEPER_URL);
			String clusterName = conf.getString(PARAM_CLUSTER_NAME);
			int zkTimeout = conf.getInt(PARAM_ZOOKEEPER_TIMEOUT, 3000);
			
			_networkClientConfig.setServiceName(clusterName);
		    _networkClientConfig.setZooKeeperConnectString(zkurl);
		    _networkClientConfig.setZooKeeperSessionTimeoutMillis(zkTimeout);
		    _networkClientConfig.setConnectTimeoutMillis(1000);
		    _networkClientConfig.setWriteTimeoutMillis(150);
		    _networkClientConfig.setMaxConnectionsPerNode(5);
		    _networkClientConfig.setStaleRequestTimeoutMins(10);
		    _networkClientConfig.setStaleRequestCleanupFrequencyMins(10);
		    
		    _clusterClient = new ZooKeeperClusterClient(clusterName,zkurl,zkTimeout);
			
		    _networkClientConfig.setClusterClient(_clusterClient);
			
			_networkClient = new SenseiNetworkClient(_networkClientConfig,_routerFactory);
			_senseiBroker = new SenseiBroker(_networkClient, _clusterClient, _reqRewriter, _routerFactory);
			
			logger.info("Connecting to cluster: "+clusterName+" ...");
			_clusterClient.awaitConnectionUninterruptibly();

			logger.info("Cluster: "+clusterName+" successfully connected ");
		} catch (ConfigurationException e) {
			throw new ServletException(e.getMessage(),e);
		}
	}
	
	protected abstract SenseiRequest buildSenseiRequest(HttpServletRequest req) throws Exception;
	
	@Override
	protected void doGet(HttpServletRequest req, HttpServletResponse resp)
			throws ServletException, IOException {
		
		try {
			SenseiRequest senseiReq = buildSenseiRequest(req);
			SenseiResult res = _senseiBroker.browse(senseiReq);
			resp.setContentType("text/plain; charset=utf-8");
			resp.setCharacterEncoding("UTF-8");
			OutputStream ostream = resp.getOutputStream();
			convertResult(senseiReq,res,ostream);
			ostream.flush();
		} catch (Exception e) {
			throw new ServletException(e.getMessage(),e);
		}
	}
	
	protected abstract void convertResult(SenseiRequest req,SenseiResult res,OutputStream ostream) throws Exception;

	@Override
	public void destroy() {
		try{
		  try{
		    if (_senseiBroker!=null){
			  _senseiBroker.shutdown();
			  _senseiBroker = null;
		    }
		  }
		  finally{
			  try{
			    if (_networkClient!=null){
			    	_networkClient.shutdown();
			    	_networkClient = null;
			    }
			  }
			  finally{
				if (_clusterClient!=null){
					_clusterClient.shutdown();
					_clusterClient = null;
				}
			  }
		  }
		  
		}
		finally{
		  super.destroy();
		}
	}
}
