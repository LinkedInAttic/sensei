package com.sensei.search.nodes;

import java.io.File;
import java.io.FilenameFilter;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.ExecutorService;

import org.apache.log4j.Logger;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.FileSystemXmlApplicationContext;

import proj.zoie.api.IndexReaderFactory;
import proj.zoie.api.ZoieIndexReader;
import proj.zoie.impl.indexing.ZoieSystem;

import com.browseengine.bobo.api.BoboIndexReader;
import com.linkedin.norbert.network.javaapi.MessageHandler;

public class SenseiServer {
	private static final Logger logger = Logger.getLogger(SenseiServer.class);
	private static final String DEFAULT_CONF_FILE = "sensei-node.spring";
	

	private ServerBootstrap _bootstrap;
	private ExecutorService _bootstrapExecutor;
	private boolean _started;
	
	private final int _port;
	
	private final SenseiSearchContext _ctx;
	private final SenseiIndexLoader _indexLoader;
	
	SenseiServer(int port,SenseiSearchContext ctx, SenseiIndexLoader indexLoader){
		_port = port;
		_started = false;
		_ctx = ctx;
		_indexLoader = indexLoader;
	}
	
	public int getPort(){
		return _port;
	}
	
	private static String help(){
		StringBuffer buffer = new StringBuffer();
		buffer.append("Usage: [id] [port] [partitions] [conf.dir]\n");
		buffer.append("====================================\n");
		buffer.append("id - node id (integer), required\n");
		buffer.append("port - server port (integer), required\n");
		buffer.append("partitions - comma separated list of partition numbers this node can serve, required\n");
		buffer.append("conf.dir - server configuration directory, required\n");
		buffer.append("====================================\n");
		return buffer.toString();
	}
	
	public static void main(String[] args) throws Exception{
		if (args.length<4){
			System.out.println(help());
			System.exit(1);
		}
		
		int id = 0;
		int port = 0;
		int[] partitions = null;
		String[] partString = null;
		
		File confDir = null;
		File confFile = null;
		
		try{
			id = Integer.parseInt(args[0]);
			port = Integer.parseInt(args[1]);
			partString = args[2].split(",");
			confDir = new File(args[3]);
			confFile = new File(confDir,DEFAULT_CONF_FILE);
			
			partitions = new int[partString.length];
			for (int i=0;i<partString.length;++i){
				partitions[i] = Integer.parseInt(partString[i]);
			}
		}
		catch(Exception e){
			System.out.println(help());
			System.exit(0);
		}
		
		
		File extDir = new File(confDir,"ext");
		File[] jarfiles = extDir.listFiles(new FilenameFilter(){

			public boolean accept(File dir, String name) {
				return name.endsWith(".jar");
			}
		});
		
		if (jarfiles!=null && jarfiles.length > 0){
		  URL[] jarURLs = new URL[jarfiles.length];
		  ClassLoader parentLoader = Thread.currentThread().getContextClassLoader();
		  for (int i=0;i<jarfiles.length;++i){
			jarURLs[i] = new URL("jar:file://" + jarfiles[i].getAbsolutePath() + "!/");  
		  }
		  URLClassLoader classloader = new URLClassLoader(jarURLs,parentLoader);
		  Thread.currentThread().setContextClassLoader(classloader);
		}
		
		ApplicationContext springCtx = new FileSystemXmlApplicationContext("file:"+confFile.getAbsolutePath());
		
		// get config parameters
		SenseiClusterConfig clusterConfig = (SenseiClusterConfig)springCtx.getBean("cluster-config");
        String clusterName = clusterConfig.getClusterName();
        String zookeeperURL = clusterConfig.getZooKeeperURL();
        
        logger.info("ClusterName: " + clusterName);
        logger.info("ZooKeeperURL: " + zookeeperURL);
        
        Map<Integer,SenseiQueryBuilderFactory> builderFactoryMap = new HashMap<Integer, SenseiQueryBuilderFactory>();
		SenseiZoieSystemFactory<?> zoieSystemFactory = (SenseiZoieSystemFactory<?>)springCtx.getBean("zoie-system-factory");
		SenseiIndexLoaderFactory indexLoaderFactory = (SenseiIndexLoaderFactory)springCtx.getBean("index-loader-factory");
		
		Map<Integer,IndexReaderFactory<ZoieIndexReader<BoboIndexReader>>> readerFactoryMap = 
				new HashMap<Integer,IndexReaderFactory<ZoieIndexReader<BoboIndexReader>>>();
		
        final HashSet<ZoieSystem<BoboIndexReader,?>> zoieSystems = new HashSet<ZoieSystem<BoboIndexReader,?>>();
        final HashSet<SenseiIndexLoader> indexLoaders = new HashSet<SenseiIndexLoader>();
		
		for (int part : partitions){
		  //in simple case query builder is the same for each partition
		  builderFactoryMap.put(part, (SenseiQueryBuilderFactory)springCtx.getBean("query-builder-factory"));
			
		  ZoieSystem<BoboIndexReader,?> zoieSystem = zoieSystemFactory.getZoieSystem(part);
		  if(!zoieSystems.contains(zoieSystem))
		  {
		    zoieSystem.start();
		    zoieSystems.add(zoieSystem);
		  }
		  
		  SenseiIndexLoader loader = indexLoaderFactory.getIndexLoader(part, zoieSystem);
		  if(!indexLoaders.contains(loader))
		  {
		    loader.start();
		    indexLoaders.add(loader);
		  }
		  readerFactoryMap.put(part, zoieSystem);
		}
		
		SenseiSearchContext ctx = new SenseiSearchContext(builderFactoryMap, readerFactoryMap);
		//SenseiServer server = new SenseiServer(port,ctx, indexLoader);
		SenseiNodeMessageHandler msgHandler = new SenseiNodeMessageHandler(ctx);
		final SenseiNode node = new SenseiNode(clusterName,id,port,new MessageHandler[] {msgHandler},zookeeperURL,partitions);
		
		node.startup();
		
		Runtime.getRuntime().addShutdownHook(new Thread(){
			public void run(){
				try {
					node.shutdown();
				} catch (Exception e) {
					logger.error(e.getMessage(),e);
				}
                // shutdown the loaders
                for(SenseiIndexLoader loader : indexLoaders)
                {
                  loader.shutdown();
                }
                // shutdown the zoieSystems
                for(ZoieSystem<BoboIndexReader,?> zoieSystem : zoieSystems)
                {
                  zoieSystem.shutdown();
                }
			}
		});
	}
}
