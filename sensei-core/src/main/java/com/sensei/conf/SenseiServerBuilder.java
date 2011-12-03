package com.sensei.conf;

import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.MapConfiguration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.DefaultSimilarity;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.Similarity;
import org.apache.lucene.util.Version;
import org.jolokia.http.AgentServlet;
import org.json.JSONException;
import org.json.JSONObject;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.nio.SelectChannelConnector;
import org.mortbay.jetty.servlet.ServletHolder;
import org.mortbay.jetty.webapp.WebAppContext;
import org.mortbay.servlet.GzipFilter;
import org.mortbay.thread.QueuedThreadPool;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.FileSystemXmlApplicationContext;
import org.springframework.core.io.Resource;
import org.w3c.dom.Document;

import proj.zoie.api.IndexCopier;
import proj.zoie.api.indexing.ZoieIndexableInterpreter;
import proj.zoie.hourglass.impl.HourGlassScheduler.FREQUENCY;
import proj.zoie.impl.indexing.ZoieConfig;
import proj.zoie.impl.HDFSIndexCopier;
import scala.actors.threadpool.Arrays;

import com.browseengine.bobo.facets.FacetHandler;
import com.browseengine.bobo.facets.RuntimeFacetHandlerFactory;
import com.linkedin.norbert.javacompat.cluster.ClusterClient;
import com.linkedin.norbert.javacompat.cluster.ZooKeeperClusterClient;
import com.linkedin.norbert.javacompat.network.NettyNetworkServer;
import com.linkedin.norbert.javacompat.network.NetworkServer;
import com.linkedin.norbert.javacompat.network.NetworkServerConfig;
import com.sensei.indexing.api.CustomIndexingPipeline;
import com.sensei.indexing.api.DefaultJsonSchemaInterpreter;
import com.sensei.indexing.api.DefaultStreamingIndexingManager;
import com.sensei.indexing.api.SenseiIndexPruner;
import com.sensei.search.client.servlet.DefaultSenseiJSONServlet;
import com.sensei.search.client.servlet.SenseiConfigServletContextListener;
import com.sensei.search.client.servlet.SenseiHttpInvokerServiceServlet;
import com.sensei.search.cluster.routing.SenseiLoadBalancerFactory;
import com.sensei.search.cluster.routing.UniformPartitionedRoutingFactory;
import com.sensei.search.nodes.SenseiCore;
import com.sensei.search.nodes.SenseiHourglassFactory;
import com.sensei.search.nodes.SenseiIndexReaderDecorator;
import com.sensei.search.nodes.SenseiIndexingManager;
import com.sensei.search.nodes.SenseiPairFactory;
import com.sensei.search.nodes.SenseiQueryBuilderFactory;
import com.sensei.search.nodes.SenseiServer;
import com.sensei.search.nodes.SenseiZoieFactory;
import com.sensei.search.nodes.SenseiZoieSystemFactory;
import com.sensei.search.nodes.impl.DefaultJsonQueryBuilderFactory;
import com.sensei.search.query.RetentionFilterFactory;
import com.sensei.search.req.AbstractSenseiRequest;
import com.sensei.search.req.AbstractSenseiResult;
import com.sensei.search.req.SenseiSystemInfo;
import com.sensei.search.svc.impl.AbstractSenseiCoreService;

public class SenseiServerBuilder implements SenseiConfParams{

  private static Logger logger = Logger.getLogger(SenseiServerBuilder.class);
  private static final String DUMMY_OUT_IP = "74.125.224.0";
  public static final String SENSEI_PROPERTIES = "sensei.properties";
  public static final String CUSTOM_FACETS = "custom-facets.xml";
  public static final String SCHEMA_FILE_XML = "schema.xml";
  public static final String SCHEMA_FILE_JSON = "schema.json";
  public static final String PLUGINS = "plugins.xml";
  
  private final File _senseiConfFile;
  private final Configuration _senseiConf;
  private final ApplicationContext _pluginContext;
  private final ApplicationContext _customFacetContext;
  private final JSONObject _schemaDoc;
  private final SenseiSchema  _senseiSchema;
  private final Server _jettyServer;
  private final Comparator<String> _versionComparator;
  
  private final static Map<String,TimeUnit> TIMEUNIT_MAP = new HashMap<String,TimeUnit>();
  static{
    TIMEUNIT_MAP.put("seconds", TimeUnit.SECONDS);
    TIMEUNIT_MAP.put("hours", TimeUnit.HOURS);
    TIMEUNIT_MAP.put("days", TimeUnit.DAYS);
  }
  
  private static ApplicationContext loadSpringContext(File[] confFiles, ApplicationContext parent){
    ApplicationContext springCtx = null;
    List<String> confList = new ArrayList<String>(confFiles.length);
    for(File confFile : confFiles)
    {
      if (confFile.exists())
        confList.add("file:"+confFile.getAbsolutePath());
    }
    if (confList.size() > 0){
      springCtx = new FileSystemXmlApplicationContext(confList.toArray(new String[confList.size()]), parent);
    }
    return springCtx;
  }

  public ClusterClient buildClusterClient()
  {
    String clusterName = _senseiConf.getString(SENSEI_CLUSTER_NAME);
	  String clusterClientName = _senseiConf.getString(SENSEI_CLUSTER_CLIENT_NAME);
    String zkUrl = _senseiConf.getString(SENSEI_CLUSTER_URL);
    int zkTimeout = _senseiConf.getInt(SENSEI_CLUSTER_TIMEOUT, 300000);
    ClusterClient clusterClient =  new ZooKeeperClusterClient(clusterClientName, clusterName, zkUrl, zkTimeout);

    logger.info("Connecting to cluster: "+clusterName+" ...");
    clusterClient.awaitConnectionUninterruptibly();

    logger.info("Cluster: "+clusterName+" successfully connected ");
    
    return clusterClient;
  }
  
  private static NetworkServer buildNetworkServer(Configuration conf,ClusterClient clusterClient){
    NetworkServerConfig networkConfig = new NetworkServerConfig();
    networkConfig.setClusterClient(clusterClient);
    networkConfig.setRequestThreadCorePoolSize(conf.getInt(SERVER_REQ_THREAD_POOL_SIZE, 20));
    networkConfig.setRequestThreadMaxPoolSize(conf.getInt(SERVER_REQ_THREAD_POOL_MAXSIZE,70));
    networkConfig.setRequestThreadKeepAliveTimeSecs(conf.getInt(SERVER_REQ_THREAD_POOL_KEEPALIVE,300));
    return new NettyNetworkServer(networkConfig);
  }
  
  static{
  try{
    org.mortbay.log.Log.setLog(new org.mortbay.log.Slf4jLog());
  }
  catch(Throwable t){
      logger.error(t.getMessage(),t);
  }
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

    AgentServlet jmxServlet = new AgentServlet();
    ServletHolder jmxServletHolder = new ServletHolder(jmxServlet);

    WebAppContext senseiApp = new WebAppContext();
    senseiApp.addFilter(GzipFilter.class,"/sensei/*",1);
    
    //HashMap<String, String> initParam = new HashMap<String, String>();
    //if (_senseiConfFile != null) {
      //logger.info("Broker Configuration file: "+_senseiConfFile.getAbsolutePath());
      //initParam.put("config.file", _senseiConfFile.getAbsolutePath());
    //}
    //senseiApp.setInitParams(initParam);
    senseiApp.setAttribute("sensei.search.configuration", _senseiConf);
    senseiApp.setAttribute("sensei.search.version.comparator", _versionComparator);

    SenseiLoadBalancerFactory routerFactory = null;
    String routerFactoryName = _senseiConf.getString(SERVER_SEARCH_ROUTER_FACTORY, "");
    if (routerFactoryName == null || routerFactoryName.equals(""))
      routerFactory = new UniformPartitionedRoutingFactory();
    else
      routerFactory = (SenseiLoadBalancerFactory) _pluginContext.getBean(routerFactoryName);

    senseiApp.setAttribute("sensei.search.router.factory", routerFactory);

    senseiApp.addEventListener(new SenseiConfigServletContextListener());
    
    
    senseiApp.addServlet(senseiServletHolder,"/sensei/*");
    //senseiApp.addFilter(new FilterHolder(new GzipFilter()), "/sensei/*", 1);
    senseiApp.setResourceBase(webappPath);
    senseiApp.addServlet(springServletHolder,"/sensei-rpc/SenseiSpringRPCService");
    senseiApp.addServlet(jmxServletHolder,"/admin/jmx/*");
    
        server.setHandler(senseiApp);
        server.setStopAtShutdown(true);  
    
    return server;
  }
  
  public static JSONObject loadSchema(File confDir) throws Exception{
    File jsonSchema = new File(confDir,SCHEMA_FILE_JSON);
    if (jsonSchema.exists()){
      InputStream is = new FileInputStream(jsonSchema);
      String json = IOUtils.toString( is );
      is.close();
      return new JSONObject(json);
    }
    else{
      File xmlSchema = new File(confDir,SCHEMA_FILE_XML);
      if (!xmlSchema.exists()){
        throw new ConfigurationException("schema not file");
      }
      DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
      dbf.setIgnoringComments(true);
      DocumentBuilder db = dbf.newDocumentBuilder();
      Document schemaXml = db.parse(xmlSchema);
      schemaXml.getDocumentElement().normalize();
      return SchemaConverter.convert(schemaXml);
    }
    
  }
  
  public SenseiServerBuilder(File confDir, Map<String, Object> properties, ApplicationContext customFacetContext,
      ApplicationContext pluginContext) throws Exception {
    if (properties != null) {
      _senseiConfFile = null;
      _senseiConf = new MapConfiguration(properties);
      ((MapConfiguration) _senseiConf).setDelimiterParsingDisabled(true);
    }
    else {
      _senseiConfFile = new File(confDir,SENSEI_PROPERTIES);
      if (!_senseiConfFile.exists()){
          throw new ConfigurationException("configuration file: "+_senseiConfFile.getAbsolutePath()+" does not exist.");
      }
      _senseiConf = new PropertiesConfiguration();
      ((PropertiesConfiguration)_senseiConf).setDelimiterParsingDisabled(true);
      ((PropertiesConfiguration)_senseiConf).load(_senseiConfFile);
    }

    if (pluginContext != null)
      _pluginContext = pluginContext;
    else
      _pluginContext = loadSpringContext(new File[] {new File(confDir,PLUGINS), new File(confDir,CUSTOM_FACETS)}, null);

    String versionComparatorName = _senseiConf.getString(SENSEI_VERSION_COMPARATOR, "");
    if (versionComparatorName == null || versionComparatorName.equals(""))
      _versionComparator = ZoieConfig.DEFAULT_VERSION_COMPARATOR;
    else
      _versionComparator = (Comparator<String>) _pluginContext.getBean(versionComparatorName);

    if (customFacetContext != null)
      _customFacetContext = customFacetContext;
    else if (pluginContext != null)
      _customFacetContext = loadSpringContext(new File[] {new File(confDir,CUSTOM_FACETS)}, _pluginContext);
    else
      _customFacetContext = _pluginContext;
    
    _schemaDoc = loadSchema(confDir);
    _senseiSchema = SenseiSchema.build(_schemaDoc);

    _jettyServer = buildHttpRestServer();
  }

  public SenseiServerBuilder(File confDir, Map<String, Object> properties, ApplicationContext customFacetContext) throws Exception {
    this(confDir, properties, customFacetContext, null);
  }

  public SenseiServerBuilder(File confDir, Map<String, Object> properties) throws Exception {
    this(confDir, properties, null, null);
  }

  public SenseiServerBuilder(File confDir) throws Exception {
    this(confDir, null, null, null);
  }

  public SenseiServerBuilder(Resource confDir, Map<String, Object> properties, ApplicationContext customFacetContext,
      ApplicationContext pluginContext) throws Exception {
    _senseiConfFile = null;

    _senseiConf = new MapConfiguration(properties);
    ((MapConfiguration) _senseiConf).setDelimiterParsingDisabled(true);

    //TODO: conditionally load other contexts.
    _pluginContext = pluginContext;

    String versionComparatorName = _senseiConf.getString(SENSEI_VERSION_COMPARATOR, "");
    if (versionComparatorName == null || versionComparatorName.equals(""))
      _versionComparator = ZoieConfig.DEFAULT_VERSION_COMPARATOR;
    else
      _versionComparator = (Comparator<String>) _pluginContext.getBean(versionComparatorName);

    _customFacetContext = customFacetContext;
    
    DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
    dbf.setIgnoringComments(true);
    DocumentBuilder db = dbf.newDocumentBuilder();
    if (confDir.createRelative(SCHEMA_FILE_JSON).exists()){
      String json = IOUtils.toString(confDir.createRelative(SCHEMA_FILE_JSON).getInputStream());
      _schemaDoc = new JSONObject(json);
    }
    else{
      if (confDir.createRelative(SCHEMA_FILE_XML).exists()){
        Document schemaXml = db.parse(confDir.createRelative(SCHEMA_FILE_XML).getInputStream());
        schemaXml.getDocumentElement().normalize();
        _schemaDoc = SchemaConverter.convert(schemaXml);
      }
      else{
        throw new Exception("no schema found.");
      }
    }
    
    _senseiSchema = SenseiSchema.build(_schemaDoc);

    _jettyServer = buildHttpRestServer();
  }

  //public SenseiServerBuilder(Resource confDir, Map<String, Object> properties, ApplicationContext customFacetContext) throws Exception {
    //this(confDir, properties, customFacetContext, null);
  //}

  //public SenseiServerBuilder(Resource confDir, Map<String, Object> properties) throws Exception {
    //this(confDir, properties, null, null);
  //}

  //public SenseiServerBuilder(Resource confDir) throws Exception {
    //this(confDir, null, null, null);
  //}

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
    String partStr = _senseiConf.getString(PARTITIONS);
    String[] partitionArray = partStr.split("[,\\s]+");
    int[] partitions = buildPartitions(partitionArray);
    logger.info("partitions to serve: "+Arrays.toString(partitions));
    
  // Analyzer from configuration:
      Analyzer analyzer = null;
      String analyzerName = _senseiConf.getString(SENSEI_INDEX_ANALYZER, "");
      if (analyzerName == null || analyzerName.equals("")) {
        analyzer = new StandardAnalyzer(Version.LUCENE_34);
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
      ZoieConfig zoieConfig = new ZoieConfig(_versionComparator);
      
      zoieConfig.setAnalyzer(analyzer);
      zoieConfig.setSimilarity(similarity);
      zoieConfig.setBatchSize(_senseiConf.getInt(SENSEI_INDEX_BATCH_SIZE,ZoieConfig.DEFAULT_SETTING_BATCHSIZE));
      zoieConfig.setBatchDelay(_senseiConf.getLong(SENSEI_INDEX_BATCH_DELAY, ZoieConfig.DEFAULT_SETTING_BATCHDELAY));
      zoieConfig.setMaxBatchSize(_senseiConf.getInt(SENSEI_INDEX_BATCH_MAXSIZE, ZoieConfig.DEFAULT_MAX_BATCH_SIZE));
      zoieConfig.setRtIndexing(_senseiConf.getBoolean(SENSEI_INDEX_REALTIME, ZoieConfig.DEFAULT_SETTING_REALTIME));
      zoieConfig.setFreshness(_senseiConf.getLong(SENSEI_INDEX_FRESHNESS, 500));

      
      List<FacetHandler<?>> facetHandlers = new LinkedList<FacetHandler<?>>();
      List<RuntimeFacetHandlerFactory<?,?>> runtimeFacetHandlerFactories = new LinkedList<RuntimeFacetHandlerFactory<?,?>>();
      
      SenseiSystemInfo sysInfo = null;
      
      try{
        sysInfo = SenseiFacetHandlerBuilder.buildFacets(_schemaDoc,_customFacetContext, facetHandlers, runtimeFacetHandlerFactories);
      }
      catch(JSONException jse){
        throw new ConfigurationException(jse.getMessage(),jse);
      }

      if (sysInfo != null)
      {
        sysInfo.setSchema(_schemaDoc.toString());

        try
        {
          List<SenseiSystemInfo.SenseiNodeInfo> clusterInfo = new ArrayList(1);

          DatagramSocket ds = new DatagramSocket();
          ds.connect(InetAddress.getByName(DUMMY_OUT_IP), 80);
          clusterInfo.add(new SenseiSystemInfo.SenseiNodeInfo(nodeid, partitions,
              (new InetSocketAddress(ds.getLocalAddress(),
                  _senseiConf.getInt(SERVER_PORT))).toString().replaceAll("/", ""),
              "http://"+(new InetSocketAddress(ds.getLocalAddress(),
                  _senseiConf.getInt(SERVER_BROKER_PORT))).toString().replaceAll("/", "")));

          sysInfo.setClusterInfo(clusterInfo);
        }
        catch(Exception e)
        {
          throw new ConfigurationException(e.getMessage(), e);
        }
      }
      
      String indexerType = _senseiConf.getString(SENSEI_INDEXER_TYPE);
      
      String interpreterType = _senseiConf.getString(SENSEI_INDEX_INTERPRETER,"");
      
      ZoieIndexableInterpreter interpreter;
      if (interpreterType.length()==0){
        DefaultJsonSchemaInterpreter defaultInterpreter = new DefaultJsonSchemaInterpreter(_senseiSchema);
        interpreter = defaultInterpreter;
        String customIndexingName = _senseiConf.getString(SENSEI_INDEX_CUSTOM,"");
        if (customIndexingName.length()>0){
          try{
            CustomIndexingPipeline customIndexingPipeline = (CustomIndexingPipeline)_pluginContext.getBean(customIndexingName);
            defaultInterpreter.setCustomIndexingPipeline(customIndexingPipeline);
          }
          catch(Exception e){
            logger.error(e.getMessage(),e);
          }
        }
      }
      else{
        interpreter = (ZoieIndexableInterpreter)_pluginContext.getBean(interpreterType);  
      } 
      
      SenseiIndexReaderDecorator decorator = new SenseiIndexReaderDecorator(facetHandlers,runtimeFacetHandlerFactories);
      File idxDir = new File(_senseiConf.getString(SENSEI_INDEX_DIR));

    
      SenseiZoieFactory<?> zoieSystemFactory = null;
      
      if (SENSEI_INDEXER_TYPE_ZOIE.equals(indexerType)){
        SenseiZoieSystemFactory senseiZoieFactory = new SenseiZoieSystemFactory(idxDir,interpreter,decorator,
                zoieConfig);

        int retentionDays = _senseiConf.getInt(SENSEI_ZOIE_RETENTION_DAYS,-1);
        if (retentionDays>0){
          String retentionClass = _senseiConf.getString(SENSEI_ZOIE_RETENTION_CLASS,null);
          Filter purgeFilter = null;
          if (retentionClass!=null){
            try{
              Class cls = Class.forName(retentionClass);
              RetentionFilterFactory retentionFilterFactory = (RetentionFilterFactory)cls.newInstance();
              purgeFilter = retentionFilterFactory.buildRetentionFilter(retentionDays);
            }
            catch(Exception e){
              throw new ConfigurationException("unable constructing retention filter", e);
            }
          }
          else{
        	  String timeColumn = _senseiConf.getString(SENSEI_ZOIE_RETENTION_COLUMN, null);
        	  if (timeColumn==null){
        	    throw new ConfigurationException("Retention specified without a time column");
        	  }
        	
        	  String unitString = _senseiConf.getString(SENSEI_ZOIE_RETENTION_TIMEUNIT,"seconds");
        	  TimeUnit unit =TIMEUNIT_MAP.get(unitString.toLowerCase());
        	
        	  if (unit == null){
        	    throw new ConfigurationException("Invalid timeunit for retention: "+unitString); 
        	  }
          }
          senseiZoieFactory.setPurgeFilter(purgeFilter);
        }
        zoieSystemFactory = senseiZoieFactory;
      }
      else if (SENSEI_INDEXER_TYPE_HOURGLASS.equals(indexerType)){
        
        String schedule = _senseiConf.getString(SENSEI_HOURGLASS_SCHEDULE,"");
        int trimThreshold = _senseiConf.getInt(SENSEI_HOURGLASS_TRIMTHRESHOLD,14);
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

      String indexerCopier = _senseiConf.getString(SENSEI_INDEXER_COPIER);

      if (indexerCopier == null || indexerCopier.length() == 0)
      {
        // do not support bootstrap index from other sources.
      }
      else if (SENSEI_INDEXER_COPIER_HDFS.equals(indexerCopier))
      {
        zoieSystemFactory = new SenseiPairFactory(idxDir,
                                                  new HDFSIndexCopier(),
                                                  interpreter,
                                                  decorator,
                                                  zoieConfig,
                                                  zoieSystemFactory);
      }
      else
      {
        IndexCopier copier = (IndexCopier)_pluginContext.getBean(indexerCopier);
        if (indexerCopier == null)
        {
          throw new ConfigurationException(indexerCopier + " not defined");
        }
        zoieSystemFactory = new SenseiPairFactory(idxDir,
                                                  copier,
                                                  interpreter,
                                                  decorator,
                                                  zoieConfig,
                                                  zoieSystemFactory);
      }

      String idxMgrType = _senseiConf.getString(SENSEI_INDEX_MANAGER,"");
      SenseiIndexingManager<?> indexingManager;
      
      if (idxMgrType.length()==0){
        String uidField = _senseiSchema.getUidField();
        indexingManager = new DefaultStreamingIndexingManager(_senseiSchema,_senseiConf, _pluginContext, _versionComparator);
      }
      else{
        indexingManager = (SenseiIndexingManager)_pluginContext.getBean(idxMgrType);  
      } 
      
      
      SenseiQueryBuilderFactory queryBuilderFactory = null;
      String qbuilderFactory = _senseiConf.getString(SENSEI_QUERY_BUILDER_FACTORY,"");
      
      if (qbuilderFactory.length()==0){
        QueryParser queryParser = new QueryParser(Version.LUCENE_34,"contents", analyzer);
        queryBuilderFactory = new DefaultJsonQueryBuilderFactory(queryParser);
      }
      else{
        queryBuilderFactory = (SenseiQueryBuilderFactory)_pluginContext.getBean(qbuilderFactory);
      }

      SenseiCore senseiCore = new SenseiCore(nodeid,partitions,zoieSystemFactory,indexingManager,queryBuilderFactory);
      senseiCore.setSystemInfo(sysInfo);
      
      
      String senseiPrunerClass = _senseiConf.getString(SENSEI_INDEX_PRUNER,"");
      if (senseiPrunerClass.length()>0){
    	  SenseiIndexPruner pruner = (SenseiIndexPruner)_pluginContext.getBean(senseiPrunerClass);
    	  senseiCore.setIndexPruner(pruner);
      }
      
      return senseiCore;
  }
  
  public Server getJettyServer(){
    return _jettyServer;
  }

  public Comparator<String> getVersionComparator() {
    return _versionComparator;
  }
  
  public SenseiServer buildServer() throws ConfigurationException { 
  int port = _senseiConf.getInt(SERVER_PORT);
    
  ClusterClient clusterClient = buildClusterClient();
    
  NetworkServer networkServer = buildNetworkServer(_senseiConf,clusterClient);
  

    SenseiCore core = buildCore();

    String externalServices = _senseiConf.getString(SENSEI_PLUGIN_SVCS);
    String[] externalServicList = externalServices.split("[,\\s]+");
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
  /*
  public HttpAdaptor buildJMXAdaptor(){
   int jmxport = _senseiConf.getInt(SENSEI_MX4J_PORT,15555);
   HttpAdaptor httpAdaptor = new HttpAdaptor(jmxport);
     httpAdaptor.setHost("0.0.0.0");   
     return httpAdaptor;
  }
  */
}
