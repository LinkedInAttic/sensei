package com.sensei.conf;

import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;

import java.io.File;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import mx4j.tools.adaptor.http.HttpAdaptor;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.DefaultSimilarity;
import org.apache.lucene.search.Similarity;
import org.apache.lucene.util.Version;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.nio.SelectChannelConnector;
import org.mortbay.jetty.servlet.ServletHolder;
import org.mortbay.jetty.webapp.WebAppContext;
import org.mortbay.thread.QueuedThreadPool;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.FileSystemXmlApplicationContext;
import org.w3c.dom.Document;

import proj.zoie.api.DefaultZoieVersion;
import proj.zoie.api.DefaultZoieVersion.DefaultZoieVersionFactory;
import proj.zoie.api.indexing.ZoieIndexableInterpreter;
import proj.zoie.hourglass.impl.HourGlassScheduler.FREQUENCY;
import proj.zoie.impl.indexing.ZoieConfig;
import scala.actors.threadpool.Arrays;

import com.browseengine.bobo.facets.FacetHandler;
import com.browseengine.bobo.facets.RuntimeFacetHandlerFactory;
import com.linkedin.norbert.javacompat.cluster.ClusterClient;
import com.linkedin.norbert.javacompat.cluster.ZooKeeperClusterClient;
import com.linkedin.norbert.javacompat.network.NettyNetworkServer;
import com.linkedin.norbert.javacompat.network.NetworkServer;
import com.linkedin.norbert.javacompat.network.NetworkServerConfig;
import com.sensei.indexing.api.DefaultJsonSchemaInterpreter;
import com.sensei.indexing.api.DefaultStreamingIndexingManager;
import com.sensei.search.client.servlet.DefaultSenseiJSONServlet;
import com.sensei.search.client.servlet.SenseiConfigServletContextListener;
import com.sensei.search.client.servlet.SenseiHttpInvokerServiceServlet;
import com.sensei.search.nodes.SenseiCore;
import com.sensei.search.nodes.SenseiHourglassFactory;
import com.sensei.search.nodes.SenseiIndexReaderDecorator;
import com.sensei.search.nodes.SenseiIndexingManager;
import com.sensei.search.nodes.SenseiQueryBuilderFactory;
import com.sensei.search.nodes.SenseiServer;
import com.sensei.search.nodes.SenseiZoieFactory;
import com.sensei.search.nodes.SenseiZoieSystemFactory;
import com.sensei.search.nodes.impl.DefaultJsonQueryBuilderFactory;
import com.sensei.search.req.AbstractSenseiRequest;
import com.sensei.search.req.AbstractSenseiResult;
import com.sensei.search.svc.impl.AbstractSenseiCoreService;

public class SenseiServerBuilder implements SenseiConfParams{

  private static Logger logger = Logger.getLogger(SenseiServerBuilder.class);
  public static final String SENSEI_PROPERTIES = "sensei.properties";
  public static final String CUSTOM_FACETS = "custom-facets.xml";
  public static final String SCHEMA_FILE = "schema.xml";
  public static final String PLUGINS = "plugins.xml";
  
  private final File _confDir;
  private final File _senseiConfFile;
  private final Configuration _senseiConf;
  private final ApplicationContext _pluginContext;
  private final ApplicationContext _customFacetContext;
  private final Document _schemaDoc;
  private final Server _jettyServer;
  
  private static ApplicationContext loadSpringContext(File confFile){
    ApplicationContext springCtx = null;
    if (confFile.exists()){
      springCtx = new FileSystemXmlApplicationContext("file:"+confFile.getAbsolutePath());
    }
    return springCtx;
  }

  private static ClusterClient buildClusterClient(Configuration conf){
	  String clusterName = conf.getString(SENSEI_CLUSTER_NAME);
	  String zkUrl = conf.getString(SENSEI_CLUSTER_URL);
	  int zkTimeout = conf.getInt(SENSEI_CLUSTER_TIMEOUT, 300000);
	  return new ZooKeeperClusterClient(clusterName, zkUrl,zkTimeout);
  }
  
  private static NetworkServer buildNetworkServer(Configuration conf,ClusterClient clusterClient){
	  NetworkServerConfig networkConfig = new NetworkServerConfig();
	  networkConfig.setClusterClient(clusterClient);
	  networkConfig.setRequestThreadCorePoolSize(conf.getInt(SERVER_REQ_THREAD_POOL_SIZE, 20));
	  networkConfig.setRequestThreadMaxPoolSize(conf.getInt(SERVER_REQ_THREAD_POOL_MAXSIZE,70));
	  networkConfig.setRequestThreadKeepAliveTimeSecs(conf.getInt(SERVER_REQ_THREAD_POOL_KEEPALIVE,300));
	  return new NettyNetworkServer(networkConfig);
  }
  
  public  Server buildHttpRestServer() throws Exception{
		int port = _senseiConf.getInt(SERVER_BROKER_PORT);
		String webappPath = _senseiConf.getString(SERVER_BROKER_WEBAPP_PATH);
		
		
		Server server = new Server();
		
		QueuedThreadPool threadPool = new QueuedThreadPool();
		threadPool.setName("Sensei Broker(jetty) threads");
		threadPool.setMinThreads(_senseiConf.getInt(SERVER_BROKER_MINTHREAD,20));
		threadPool.setMaxThreads(_senseiConf.getInt(SERVER_BROKER_MAXTHREAD,50));
		threadPool.setMaxIdleTimeMs(_senseiConf.getInt(SERVER_BROKER_MAXWAIT,2000));
		//threadPool.start();
		server.setThreadPool(threadPool);

		logger.info("request threadpool started.");
		SelectChannelConnector connector = new SelectChannelConnector();
		connector.setPort(port);
	    server.addConnector(connector);
		
		DefaultSenseiJSONServlet senseiServlet = new DefaultSenseiJSONServlet();
		ServletHolder senseiServletHolder = new ServletHolder(senseiServlet);

		SenseiHttpInvokerServiceServlet springServlet = new SenseiHttpInvokerServiceServlet();
		ServletHolder springServletHolder = new ServletHolder(springServlet);

		WebAppContext senseiApp = new WebAppContext();
		
		HashMap<String,String> initParam = new HashMap<String,String>();
		initParam.put("config.file", _senseiConfFile.getAbsolutePath());
		senseiApp.setInitParams(initParam);
		senseiApp.addEventListener(new SenseiConfigServletContextListener());
		
		
		senseiApp.addServlet(senseiServletHolder,"/sensei/*");
		senseiApp.setResourceBase(webappPath);
		senseiApp.addServlet(springServletHolder,"/sensei-rpc/SenseiSpringRPCService");
		
        server.setHandler(senseiApp);
        server.setStopAtShutdown(true);	
		
		return server;
	}
  
  public SenseiServerBuilder(File confDir) throws Exception{
	  _confDir = confDir;
	  _senseiConfFile = new File(confDir,SENSEI_PROPERTIES);
	  if (!_senseiConfFile.exists()){
        throw new ConfigurationException("configuration file: "+_senseiConfFile.getAbsolutePath()+" does not exist.");
	  }
	  _senseiConf = new PropertiesConfiguration(_senseiConfFile);

	  _jettyServer = buildHttpRestServer();
	  _pluginContext = loadSpringContext(new File(confDir,PLUGINS));
	  _customFacetContext = loadSpringContext(new File(confDir,CUSTOM_FACETS));
	  
	  DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
	  dbf.setValidating(true);
	  dbf.setIgnoringComments(true);
	  DocumentBuilder db = dbf.newDocumentBuilder();
	  _schemaDoc = db.parse(new File(_confDir,SCHEMA_FILE));
	  _schemaDoc.getDocumentElement().normalize();
  }
  
  static final Pattern PARTITION_PATTERN = Pattern.compile("[\\d]+||[\\d]+-[\\d]+");
  
  public static int[] buildPartitions(String[] partitionArray) throws ConfigurationException{
	  IntSet partitions = new IntOpenHashSet();
	  try {
	    for (int i=0; i<partitionArray.length; ++i) {
	      Matcher matcher = PARTITION_PATTERN.matcher(partitionArray[i]);
	      if (!matcher.matches()){
	    	  throw new ConfigurationException("Invalid partition: "+partitionArray[i]);
	      }
	      String[] partitionRange = partitionArray[i].split("-");
	      int start = Integer.parseInt(partitionRange[0]);
	      int end;
	      if (partitionRange.length>1){
	    	  end = Integer.parseInt(partitionRange[1]);
		      if (end<start){
		    	  throw new ConfigurationException("invalid partition range: "+partitionArray[i]);
		      }
	      }
	      else{
	    	  end = start;
	      }
	      
	      for (int k=start;k<=end;++k){
	    	  partitions.add(k);
	      }
	    }
	  }
	  catch (Exception e) {
	    throw new ConfigurationException(
	        "Error parsing '" + SENSEI_PROPERTIES + "': " + PARTITIONS + "=" + Arrays.toString(partitionArray), e);
	  }

	  return partitions.toIntArray();
  }
  
  public SenseiCore buildCore() throws ConfigurationException{
	  int nodeid = _senseiConf.getInt(NODE_ID);
	  String[] partitionArray = _senseiConf.getStringArray(PARTITIONS);
	  int[] partitions = buildPartitions(partitionArray);
	  logger.info("partitions to serve: "+Arrays.toString(partitions));
	  File extDir = new File(_confDir,"ext");
	  
	// Analyzer from configuration:
      Analyzer analyzer = null;
      String analyzerName = _senseiConf.getString(SENSEI_INDEX_ANALYZER, "");
      if (analyzerName == null || analyzerName.equals("")) {
        analyzer = new StandardAnalyzer(Version.LUCENE_29);
      }
      else {
        analyzer = (Analyzer)_pluginContext.getBean(analyzerName);
      }

      // Similarity from configuration:
      Similarity similarity = null;
      String similarityName = _senseiConf.getString(SENSEI_INDEX_SIMILARITY, "");
      if (similarityName == null || similarityName.equals("")) {
        similarity = new DefaultSimilarity();
      }
      else {
        similarity = (Similarity)_pluginContext.getBean(similarityName);
      }
      DefaultZoieVersionFactory defaultVersionFac = new DefaultZoieVersion.DefaultZoieVersionFactory();
      
      ZoieConfig<DefaultZoieVersion> zoieConfig = new ZoieConfig<DefaultZoieVersion>(defaultVersionFac);
      
      zoieConfig.setAnalyzer(analyzer);
      zoieConfig.setSimilarity(similarity);
      zoieConfig.setBatchSize(_senseiConf.getInt(SENSEI_INDEX_BATCH_SIZE,ZoieConfig.DEFAULT_SETTING_BATCHSIZE));
      zoieConfig.setBatchDelay(_senseiConf.getLong(SENSEI_INDEX_BATCH_DELAY, ZoieConfig.DEFAULT_SETTING_BATCHDELAY));
      zoieConfig.setMaxBatchSize(_senseiConf.getInt(SENSEI_INDEX_BATCH_MAXSIZE, ZoieConfig.DEFAULT_MAX_BATCH_SIZE));
      zoieConfig.setRtIndexing(_senseiConf.getBoolean(SENSEI_INDEX_REALTIME, ZoieConfig.DEFAULT_SETTING_REALTIME));
      zoieConfig.setFreshness(_senseiConf.getLong(SENSEI_INDEX_FRESHNESS, 10000));

      QueryParser queryParser = new QueryParser(Version.LUCENE_29,"contents", analyzer);

      List<FacetHandler<?>> facetHandlers = new LinkedList<FacetHandler<?>>();
      List<RuntimeFacetHandlerFactory<?,?>> runtimeFacetHandlerFactories = new LinkedList<RuntimeFacetHandlerFactory<?,?>>();
      
      SenseiFacetHandlerBuilder.buildFacets(_schemaDoc, _customFacetContext, facetHandlers, runtimeFacetHandlerFactories);
      
      String indexerType = _senseiConf.getString(SENSEI_INDEXER_TYPE);
      
      String interpreterType = _senseiConf.getString(SENSEI_INDEX_INTERPRETER,"");
      
      SenseiSchema senseiSchma = SenseiSchema.build(_schemaDoc);
      ZoieIndexableInterpreter interpreter;
      if (interpreterType.length()==0){
    	interpreter = new DefaultJsonSchemaInterpreter(senseiSchma);
      }
      else{
    	interpreter = (ZoieIndexableInterpreter)_pluginContext.getBean(interpreterType);  
      } 
      
      SenseiIndexReaderDecorator decorator = new SenseiIndexReaderDecorator(facetHandlers,runtimeFacetHandlerFactories);
      File idxDir = new File(_senseiConf.getString(SENSEI_INDEX_DIR));

    
      SenseiZoieFactory<?,?> zoieSystemFactory = null;
      
      if (SENSEI_INDEXER_TYPE_ZOIE.equals(indexerType)){
    	  zoieSystemFactory = new SenseiZoieSystemFactory(idxDir,interpreter,decorator,
    		        zoieConfig);
      }
      else if (SENSEI_INDEXER_TYPE_HOURGLASS.equals(indexerType)){
    	  
    	  String schedule = _senseiConf.getString(SENSEI_HOURGLASS_SCHEDULE,"");
    	  int trimThreshold = _senseiConf.getInt(SENSEI_HOURGLASS_TIMETHRESHOLD,14);
    	  String frequencyString = _senseiConf.getString(SENSEI_HOURGLASS_FREQUENCY,"day");
    	  
    	  FREQUENCY frequency;
    	  
    	  if (SENSEI_HOURGLASS_FREQUENCY_MIN.equals(frequencyString)){
    		  frequency = FREQUENCY.MINUTELY;
    	  }
    	  else if (SENSEI_HOURGLASS_FREQUENCY_HOUR.equals(frequencyString)){
    		  frequency = FREQUENCY.HOURLY;
    	  }
    	  else if (SENSEI_HOURGLASS_FREQUENCY_DAY.equals(frequencyString)){
    		  frequency = FREQUENCY.DAILY;
    	  }
    	  else{
    		  throw new ConfigurationException("unsupported frequency setting: "+frequencyString);
    	  }
    	  zoieSystemFactory = new SenseiHourglassFactory(idxDir,interpreter,decorator,
  		        zoieConfig,schedule,trimThreshold,frequency);
      }
      else{
    	  ZoieFactoryFactory zoieFactoryFactory= (ZoieFactoryFactory)_pluginContext.getBean(indexerType);
    	  if (zoieFactoryFactory==null){
    		  throw new ConfigurationException(indexerType+" not defined");
    	  }
    	  zoieSystemFactory = zoieFactoryFactory.getZoieFactory(idxDir, interpreter, decorator, zoieConfig);
      }
      
      String idxMgrType = _senseiConf.getString(SENSEI_INDEX_MANAGER,"");
      SenseiIndexingManager<?,DefaultZoieVersion> indexingManager;
      
      if (idxMgrType.length()==0){
    	String uidField = senseiSchma.getUidField();
    	indexingManager = new DefaultStreamingIndexingManager(senseiSchma,_senseiConf);
      }
      else{
    	indexingManager = (SenseiIndexingManager)_pluginContext.getBean(idxMgrType);  
      } 
      
      SenseiQueryBuilderFactory queryBuilderFactory = new DefaultJsonQueryBuilderFactory(queryParser);
      
      return new SenseiCore(nodeid,partitions,extDir,zoieSystemFactory,indexingManager,queryBuilderFactory);
  }
  
  public Server getJettyServer(){
	  return _jettyServer;
  }
  
  public SenseiServer buildServer() throws ConfigurationException { 
	int port = _senseiConf.getInt(SERVER_PORT);
	  
	ClusterClient clusterClient = buildClusterClient(_senseiConf);
	NetworkServer networkServer = buildNetworkServer(_senseiConf,clusterClient);
	

    SenseiCore core = buildCore();
    
    //String versionComparatorParam = _senseiConf.getString(SenseiConfParams.SENSEI_VERSION_COMPARATOR,"");
    //Comparator<String> versionComparator;
    
   // if (versionComparatorParam.length()==0 || "long".equals(versionComparatorParam)){
    //	versionComparator = SenseiConfParams.DEFAULT_VERSION_LONG_COMPARATOR;
    	
    //}
    /*else if ("string".equals(versionComparatorParam)){
    	versionComparator = SenseiConfParams.DEFAULT_VERSION_STRING_COMPARATOR;
    }
    else{
    	versionComparator = (Comparator<String>)_pluginContext.getBean(versionComparatorParam);
    }*/
    
   // final Comparator<String> zoieComparator = versionComparator;

    String[] externalServicList = _senseiConf.getStringArray(SENSEI_PLUGIN_SVCS);
    List<AbstractSenseiCoreService<AbstractSenseiRequest, AbstractSenseiResult>> svcList = new LinkedList<AbstractSenseiCoreService<AbstractSenseiRequest, AbstractSenseiResult>>();
    if (externalServicList!=null){
      for (String svcName : externalServicList){
    	  if (svcName!=null && svcName.trim().length()>0){
    	    try{
    		  AbstractSenseiCoreService<AbstractSenseiRequest, AbstractSenseiResult> svc = (AbstractSenseiCoreService<AbstractSenseiRequest, AbstractSenseiResult>)_pluginContext.getBean(svcName);
    		  svcList.add(svc);
    	    }
    	    catch(Exception e){
    		  throw new ConfigurationException(e.getMessage(),e);
    	    }
    	  }
      }
    }
	return new SenseiServer(port,networkServer,clusterClient,core,svcList);
  }
  
  public HttpAdaptor buildJMXAdaptor(){
	 int jmxport = _senseiConf.getInt(SENSEI_MX4J_PORT,15555);
	 HttpAdaptor httpAdaptor = new HttpAdaptor(jmxport);
     httpAdaptor.setHost("0.0.0.0");   
     return httpAdaptor;
  }
}
