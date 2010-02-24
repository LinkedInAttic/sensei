package com.sensei.search.nodes;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;

import java.io.File;
import java.io.FilenameFilter;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;

import org.apache.log4j.Logger;
import org.apache.lucene.queryParser.QueryParser;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.FileSystemXmlApplicationContext;

import proj.zoie.api.IndexReaderFactory;
import proj.zoie.api.ZoieIndexReader;

import com.browseengine.bobo.api.BoboIndexReader;
import com.sensei.search.req.RuntimeFacetHandlerFactory;

public class SenseiServer {
	private static final Logger logger = Logger.getLogger(SenseiServer.class);
	private static final String DEFAULT_CONF_FILE = "sensei-node.spring";
	

	private static final String DEFAULT_ZK_URL = "localhost:2181";
	
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
		buffer.append("Usage: [id] [port] [partitions] [conf.dir] <zookeeper url>\n");
		buffer.append("====================================\n");
		buffer.append("id - node id (integer), required\n");
		buffer.append("port - server port (integer), required\n");
		buffer.append("partitions - comma separated list of partition numbers this node can serve, required\n");
		buffer.append("conf.dir - server configuration directory, required\n");
		buffer.append("zookeeper url - url (form: host:port) of the zookeeper instance/cluster, optional, default: ").append(DEFAULT_ZK_URL).append("\n");
		buffer.append("====================================\n");
		return buffer.toString();
	}
	
	public static final String Cluster_Name="sensei";
	
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
		String zookeeperURL = null;
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
			
			
			try{
				zookeeperURL = args[4];
			}
			catch(Exception e){
				zookeeperURL = null;
			}
			
			if (zookeeperURL == null){
				System.out.println("invalid or no cluster url specified, defaulting to default: "+DEFAULT_ZK_URL);
				zookeeperURL = DEFAULT_ZK_URL;
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
		
		QueryParser qparser = (QueryParser)springCtx.getBean("query-parser");
		IndexReaderFactory<ZoieIndexReader<BoboIndexReader>> idxReaderFactory = (IndexReaderFactory<ZoieIndexReader<BoboIndexReader>>)springCtx.getBean("index-reader-factory");
		List<RuntimeFacetHandlerFactory<?>> runtimeFacethandlerFactories = (List<RuntimeFacetHandlerFactory<?>>)springCtx.getBean("runtime-facet-handler-factories");
		SenseiIndexLoader indexLoader = (SenseiIndexLoader)springCtx.getBean("index-loader");
		
		Int2ObjectMap<IndexReaderFactory<ZoieIndexReader<BoboIndexReader>>> readerFactoryMap = 
				new Int2ObjectOpenHashMap<IndexReaderFactory<ZoieIndexReader<BoboIndexReader>>>();
		
		//TODO: THIS HARDCODING NEEDS TO BE FIXED!!!
		for (int i=0;i<100;++i){
			readerFactoryMap.put(i, idxReaderFactory);
		}
		
		SenseiSearchContext ctx = new SenseiSearchContext(qparser, readerFactoryMap, runtimeFacethandlerFactories);
		//SenseiServer server = new SenseiServer(port,ctx, indexLoader);
		SenseiNodeMessageHandler msgHandler = new SenseiNodeMessageHandler(ctx);
		final SenseiNode node = new SenseiNode(Cluster_Name,id,port,partitions,msgHandler,zookeeperURL);
		node.startup();
		
		
		Runtime.getRuntime().addShutdownHook(new Thread(){
			public void run(){
				try {
					node.shutdown();
				} catch (Exception e) {
					logger.error(e.getMessage(),e);
				}
			}
		});
	}
}
