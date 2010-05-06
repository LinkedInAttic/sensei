package com.sensei.search.nodes;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import java.lang.management.ManagementFactory;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import javax.management.StandardMBean;

import org.apache.log4j.Logger;

import proj.zoie.api.IndexReaderFactory;
import proj.zoie.api.ZoieIndexReader;
import proj.zoie.impl.indexing.ZoieSystem;
import proj.zoie.mbean.ZoieIndexingStatusAdmin;
import proj.zoie.mbean.ZoieIndexingStatusAdminMBean;
import proj.zoie.mbean.ZoieSystemAdminMBean;

import com.browseengine.bobo.api.BoboIndexReader;
import com.linkedin.norbert.network.javaapi.MessageHandler;
import com.sensei.search.svc.api.SenseiException;

/**
 *
 * EmbeddedSenseiServer is a refactoring of the SenseiServer
 * where all the configuration is pushed in from the outside
 * and the server is started via the init-method "start" and
 * stopped by the destroy-method "shutdown"
 *
 * The motivation is for enviroments (eg: webapps) where the 
 * Sensei node's lifecyle can be more sensibly managed by the
 * container rather than as a standalone application.
 *
 * See node-conf/sensei-embed.spring for an example
 *
 * @author Brian Hammond
 *
 */
public class EmbeddedSenseiServer {
	private static final Logger logger = Logger.getLogger( EmbeddedSenseiServer.class );
	private static final String AVAILABLE = "available";
	private static final String UNAVAILABLE = "unavailable";	

	private SenseiNode _node;
	
	Set<ZoieSystem<BoboIndexReader,?>> zoieSystems;
	Set<SenseiIndexLoader> indexLoaders;

	private int _id;
	private int _port;
	private int[] _partitions;
	private SenseiClusterConfig clusterConfig;
	private SenseiZoieSystemFactory<?> zoieSystemFactory;
	private SenseiIndexLoaderFactory indexLoaderFactory;
	private Map<Integer,SenseiQueryBuilderFactory> builderFactoryMap;
	private Map< Integer, ZoieSystem<BoboIndexReader,?> > zoieSystemMap;
	private boolean available_ = true;

	////

	public EmbeddedSenseiServer() {
	}

	public EmbeddedSenseiServer( int id, int port, int[] partitions ) {
		this.setId( id );
		this.setPort( port );
		this.setPartitions( partitions );
	}

	////

	public void start() throws Exception {
		this.start( this.getAvailable() );
	}

	private void start(boolean available) throws Exception {
		// get config parameters
		String clusterName = clusterConfig.getClusterName();
		String zookeeperURL = clusterConfig.getZooKeeperURL();
		int zooKeeperSessionTimeout = clusterConfig.getZooKeeperSessionTimeoutMillis();
		
		logger.info("ClusterName: " + clusterName);
		logger.info("ZooKeeperURL: " + zookeeperURL);

		Map<Integer,IndexReaderFactory<ZoieIndexReader<BoboIndexReader>>> readerFactoryMap = (
			new HashMap<Integer,IndexReaderFactory<ZoieIndexReader<BoboIndexReader>>>()
		);

		this.zoieSystems = new HashSet<ZoieSystem<BoboIndexReader,?>>();
		this.indexLoaders = new HashSet<SenseiIndexLoader>();

		MBeanServer mbeanServer = ManagementFactory.getPlatformMBeanServer();

		// TODO: configure more of this with spring
		for ( int part : _partitions ){
			ZoieSystem<BoboIndexReader,?> zoieSystem = zoieSystemMap.get( part );

			// register ZoieSystemAdminMBean
			mbeanServer.registerMBean(
				new StandardMBean(zoieSystem.getAdminMBean(), ZoieSystemAdminMBean.class)
				, new ObjectName(clusterName, "name", "zoie-system-" + part)
			);

			// register ZoieIndexingStatusAdminMBean
			mbeanServer.registerMBean(
					new StandardMBean(
						new ZoieIndexingStatusAdmin(zoieSystem), ZoieIndexingStatusAdminMBean.class
						)
					, new ObjectName(clusterName, "name", "zoie-indexing-status-" + part )
					);


			// beans for zoieSystem should use "start" as their init-methods:
			zoieSystems.add(zoieSystem);

			SenseiIndexLoader loader = indexLoaderFactory.getIndexLoader(part, zoieSystem);
			if(!indexLoaders.contains(loader))
			{
				loader.start();
				indexLoaders.add(loader);
			}
			readerFactoryMap.put(part, zoieSystem);
		}

		SenseiSearchContext ctx = new SenseiSearchContext(builderFactoryMap, readerFactoryMap);
		SenseiNodeMessageHandler msgHandler = new SenseiNodeMessageHandler(ctx);
		_node = new SenseiNode(clusterName,_id,_port, msgHandler,zookeeperURL,_partitions, zooKeeperSessionTimeout);

		_node.startup( available );

		SenseiServerAdminMBean mbean = getAdminMBean();

		mbeanServer.registerMBean(
			new StandardMBean(mbean, SenseiServerAdminMBean.class)
			, new ObjectName(clusterName, "name", "sensei-server")
		);
	
		//Runtime.getRuntime().addShutdownHook( new ShutdownHook( this ) );
		logger.info( "started" );
	}

	public void shutdown() {
		try {
			_node.shutdown();
		} catch (Exception e) {
			logger.error(e.getMessage(),e);
		}

		// shutdown the loaders
		for(SenseiIndexLoader loader : indexLoaders) {
			try{
				loader.shutdown();
			} catch(SenseiException se){
				logger.error(se.getMessage(),se);
			}
		}

		// shutdown the zoieSystems
		for(ZoieSystem<BoboIndexReader,?> zoieSystem : zoieSystems) {
			zoieSystem.shutdown();
		}
		System.out.println( "bye..." );
	}

	public class ShutdownHook extends Thread {
		EmbeddedSenseiServer embeddedSenseiServer;
		public ShutdownHook( EmbeddedSenseiServer embeddedSenseiServer ) {
			this.embeddedSenseiServer = embeddedSenseiServer;
		}

		public void run(){
			this.embeddedSenseiServer.shutdown();
		}
	};

	private SenseiServerAdminMBean getAdminMBean() {
		//brian:lame...
		boolean first = true;
		StringBuilder stringBuilder = new StringBuilder();
		for ( int partition : this.getPartitions() ) {
			if ( first ) { 
				first = false;
			} else {
				stringBuilder.append( "," );
			}
			stringBuilder.append( partition );
		}
		final String _partitionString = stringBuilder.toString();

		return new SenseiServerAdminMBean()
		{
			public int getId()
			{
				return _id;
			}
			public int getPort()
			{
				return _port;
			}
			public String getPartitions()
			{
				return _partitionString;
			}
			public boolean isAvailable()
			{
				return _node.isAvailable();
			}
			public void setAvailable(boolean available)
			{
				_node.setAvailable(available);
			}
		};
	}

	////

	public int getId() {
		return this._id;
	}

	public void setId( int id ) {
		this._id = id;
	}

	public int getPort() {
		return this._port;
	}

	public void setPort( int port ) {
		this._port = port;
	}

	public int[] getPartitions() {
		return this._partitions;
	}

	public void setPartitions( int partitions[] ) {
		this._partitions = partitions;
	}

	public boolean getAvailable() {
		return this.available_;
	}

	public void setAvailable( boolean available ) {
		this.available_ = available;
	}

	public Map< Integer, ZoieSystem<BoboIndexReader,?> > getZoieSystemMap() {
		return this.zoieSystemMap;
	}

	public void setZoieSystemMap( Map< Integer, ZoieSystem<BoboIndexReader,?> > zoieSystemMap ) {
		this.zoieSystemMap = zoieSystemMap;
	}

	public SenseiClusterConfig getClusterConfig() {
		return this.clusterConfig;
	}

	public void setClusterConfig( SenseiClusterConfig clusterConfig ) {
		this.clusterConfig = clusterConfig;
	}

	public SenseiZoieSystemFactory<?> getZoieSystemFactory() {
		return this.zoieSystemFactory;
	}

	public void setZoieSystemFactory( SenseiZoieSystemFactory<?> zoieSystemFactory ) {
		this.zoieSystemFactory = zoieSystemFactory;
	}

	public SenseiIndexLoaderFactory getIndexLoaderFactory() {
		return this.indexLoaderFactory;
	}

	public void setIndexLoaderFactory( SenseiIndexLoaderFactory indexLoaderFactory ) {
		this.indexLoaderFactory = indexLoaderFactory;
	}

	public Map<Integer,SenseiQueryBuilderFactory> getBuilderFactoryMap() {
		return this.builderFactoryMap;
	}

	public void setBuilderFactoryMap( Map<Integer,SenseiQueryBuilderFactory> builderFactoryMap ) {
		this.builderFactoryMap = builderFactoryMap;
	}
};
