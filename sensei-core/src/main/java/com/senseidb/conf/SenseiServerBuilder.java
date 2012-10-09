package com.senseidb.conf;

import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
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
import org.springframework.core.io.Resource;
import org.w3c.dom.Document;

import proj.zoie.api.DirectoryManager.DIRECTORY_MODE;
import proj.zoie.api.IndexCopier;
import proj.zoie.api.indexing.ZoieIndexableInterpreter;
import proj.zoie.hourglass.impl.HourGlassScheduler.FREQUENCY;
import proj.zoie.impl.indexing.DefaultReaderCache;
import proj.zoie.impl.indexing.ReaderCacheFactory;
import proj.zoie.impl.indexing.SimpleReaderCache;
import proj.zoie.impl.indexing.ZoieConfig;

import com.browseengine.bobo.facets.FacetHandler;
import com.browseengine.bobo.facets.RuntimeFacetHandlerFactory;
import com.linkedin.norbert.javacompat.cluster.ClusterClient;
import com.linkedin.norbert.javacompat.cluster.ZooKeeperClusterClient;
import com.linkedin.norbert.javacompat.network.NettyNetworkServer;
import com.linkedin.norbert.javacompat.network.NetworkServer;
import com.linkedin.norbert.javacompat.network.NetworkServerConfig;
import com.linkedin.norbert.javacompat.network.PartitionedLoadBalancerFactory;
import com.senseidb.cluster.routing.SenseiPartitionedLoadBalancerFactory;
import com.senseidb.gateway.SenseiGateway;
import com.senseidb.indexing.CustomIndexingPipeline;
import com.senseidb.indexing.DefaultJsonSchemaInterpreter;
import com.senseidb.indexing.DefaultStreamingIndexingManager;
import com.senseidb.indexing.SenseiIndexPruner;
import com.senseidb.indexing.ShardingStrategy;
import com.senseidb.indexing.activity.deletion.PurgeFilterWrapper;
import com.senseidb.jmx.JmxSenseiMBeanServer;
import com.senseidb.plugin.SenseiPluginRegistry;
import com.senseidb.search.node.SenseiCore;
import com.senseidb.search.node.SenseiHourglassFactory;
import com.senseidb.search.node.SenseiIndexReaderDecorator;
import com.senseidb.search.node.SenseiIndexingManager;
import com.senseidb.search.node.SenseiPairFactory;
import com.senseidb.search.node.SenseiQueryBuilderFactory;
import com.senseidb.search.node.SenseiServer;
import com.senseidb.search.node.SenseiZoieFactory;
import com.senseidb.search.node.SenseiZoieSystemFactory;
import com.senseidb.search.node.impl.DefaultJsonQueryBuilderFactory;
import com.senseidb.search.plugin.PluggableSearchEngineManager;
import com.senseidb.search.query.RetentionFilterFactory;
import com.senseidb.search.query.TimeRetentionFilter;
import com.senseidb.search.relevance.CustomRelevanceFunction.CustomRelevanceFunctionFactory;
import com.senseidb.search.relevance.ExternalRelevanceDataStorage;
import com.senseidb.search.relevance.ExternalRelevanceDataStorage.RelevanceObjPlugin;
import com.senseidb.search.relevance.ModelStorage;
import com.senseidb.search.req.AbstractSenseiRequest;
import com.senseidb.search.req.AbstractSenseiResult;
import com.senseidb.search.req.SenseiSystemInfo;
import com.senseidb.servlet.DefaultSenseiJSONServlet;
import com.senseidb.servlet.SenseiConfigServletContextListener;
import com.senseidb.servlet.SenseiHttpInvokerServiceServlet;
import com.senseidb.svc.impl.AbstractSenseiCoreService;
import com.senseidb.util.HDFSIndexCopier;
import com.senseidb.util.NetUtil;
import com.senseidb.util.SenseiUncaughtExceptionHandler;
import com.senseidb.util.JSONUtil.FastJSONArray;
import com.senseidb.util.JSONUtil.FastJSONObject;

public class SenseiServerBuilder implements SenseiConfParams{

  private static Logger logger = Logger.getLogger(SenseiServerBuilder.class);
  private static final String DUMMY_OUT_IP = "74.125.224.0";
  public static final String SENSEI_PROPERTIES = "sensei.properties";

  public static final String SCHEMA_FILE_XML = "schema.xml";
  public static final String SCHEMA_FILE_JSON = "schema.json";


  private final File _senseiConfFile;
  private final Configuration _senseiConf;
  private SenseiPluginRegistry pluginRegistry;

  private final JSONObject _schemaDoc;
  private final SenseiSchema  _senseiSchema;
  private final SenseiGateway _gateway;
  private PluggableSearchEngineManager pluggableSearchEngineManager;
  private SenseiIndexReaderDecorator decorator;

  static final String SENSEI_CONTEXT_PATH = "sensei";


  public Configuration getConfiguration(){
    return _senseiConf;
  }

  public SenseiPluginRegistry getPluginRegistry(){
    return pluginRegistry;
  }


  public ClusterClient buildClusterClient()
  {
    String clusterName = _senseiConf.getString(SENSEI_CLUSTER_NAME);
    String clusterClientName = _senseiConf.getString(SENSEI_CLUSTER_CLIENT_NAME,clusterName);
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

    String webappPath = _senseiConf.getString(SERVER_BROKER_WEBAPP_PATH,"sensei-core/src/main/webapp");


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
    senseiApp.addFilter(GzipFilter.class,"/"+SENSEI_CONTEXT_PATH+"/*",1);

    //HashMap<String, String> initParam = new HashMap<String, String>();
    //if (_senseiConfFile != null) {
    //logger.info("Broker Configuration file: "+_senseiConfFile.getAbsolutePath());
    //initParam.put("config.file", _senseiConfFile.getAbsolutePath());
    //}
    //senseiApp.setInitParams(initParam);
    senseiApp.setAttribute("sensei.search.configuration", _senseiConf);
    senseiApp.setAttribute(SenseiConfigServletContextListener.SENSEI_CONF_PLUGIN_REGISTRY, pluginRegistry);
    senseiApp.setAttribute("sensei.search.version.comparator", _gateway != null ? _gateway.getVersionComparator() : ZoieConfig.DEFAULT_VERSION_COMPARATOR);

    PartitionedLoadBalancerFactory<String> routerFactory = pluginRegistry.getBeanByFullPrefix(SenseiConfParams.SERVER_SEARCH_ROUTER_FACTORY, PartitionedLoadBalancerFactory.class);
    if (routerFactory == null) {
      routerFactory = new SenseiPartitionedLoadBalancerFactory(50);
    }

    senseiApp.setAttribute("sensei.search.router.factory", routerFactory);
    senseiApp.addEventListener(new SenseiConfigServletContextListener());
    senseiApp.addServlet(senseiServletHolder,"/"+SENSEI_CONTEXT_PATH+"/*");
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
      return new FastJSONObject(json);
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

  public static JSONObject loadSchema(Resource confDir) throws Exception
  {
    if (confDir.createRelative(SCHEMA_FILE_JSON).exists()){
      String json = IOUtils.toString(confDir.createRelative(SCHEMA_FILE_JSON).getInputStream());
      return new FastJSONObject(json);
    }
    else{
      if (confDir.createRelative(SCHEMA_FILE_XML).exists()){
        DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
        dbf.setIgnoringComments(true);
        DocumentBuilder db = dbf.newDocumentBuilder();
        Document schemaXml = db.parse(confDir.createRelative(SCHEMA_FILE_XML).getInputStream());
        schemaXml.getDocumentElement().normalize();
        return SchemaConverter.convert(schemaXml);
      }
      else{
        throw new Exception("no schema found.");
      }
    }
  }

  public SenseiServerBuilder(File confDir) throws Exception {
    this(confDir, null);
  }

  public SenseiServerBuilder(File confDir, Map<String, Object> properties) throws Exception {
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

    pluginRegistry = SenseiPluginRegistry.build(_senseiConf);
    pluginRegistry.start();
    
    processRelevanceFunctionPlugins(pluginRegistry);
    processRelevanceExternalObjectPlugins(pluginRegistry);

    _gateway = pluginRegistry.getBeanByFullPrefix(SENSEI_GATEWAY, SenseiGateway.class);
    _schemaDoc = loadSchema(confDir);
    _senseiSchema = SenseiSchema.build(_schemaDoc);
  }


  public SenseiServerBuilder(Resource confDir, Map<String, Object> properties) throws Exception
  {
    _senseiConfFile = null;

    _senseiConf = new MapConfiguration(properties);
    ((MapConfiguration) _senseiConf).setDelimiterParsingDisabled(true);

    pluginRegistry = SenseiPluginRegistry.build(_senseiConf);
    pluginRegistry.start();
    
    processRelevanceFunctionPlugins(pluginRegistry);

    _gateway = pluginRegistry.getBeanByFullPrefix(SENSEI_GATEWAY, SenseiGateway.class);

    _schemaDoc = loadSchema(confDir);
    _senseiSchema = SenseiSchema.build(_schemaDoc);
  }

  private void processRelevanceFunctionPlugins(SenseiPluginRegistry pluginRegistry)
  {
    Map<String, CustomRelevanceFunctionFactory> map = pluginRegistry.getNamedBeansByType(CustomRelevanceFunctionFactory.class);
    Iterator<String> it = map.keySet().iterator();
    while(it.hasNext())
    {
      String name = it.next();
      CustomRelevanceFunctionFactory crf = map.get(name);
      ModelStorage.injectPreloadedModel(name, crf);
    }
  }
  

  private void processRelevanceExternalObjectPlugins(SenseiPluginRegistry pluginRegistry)
  {
    List<RelevanceObjPlugin> relObjPlugins = pluginRegistry.getBeansByType(RelevanceObjPlugin.class);
    for(RelevanceObjPlugin rop : relObjPlugins)
      ExternalRelevanceDataStorage.putObj(rop);
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

    int[] ret = partitions.toIntArray();
    Arrays.sort(ret);
    return ret;
  }
  
  public SenseiCore buildCore() throws ConfigurationException {
    SenseiUncaughtExceptionHandler.setAsDefaultForAllThreads();
    int nodeid = _senseiConf.getInt(NODE_ID);
    String partStr = _senseiConf.getString(PARTITIONS);
    String[] partitionArray = partStr.split("[,\\s]+");
    int[] partitions = buildPartitions(partitionArray);
    logger.info("partitions to serve: "+Arrays.toString(partitions));
  // Analyzer from configuration:
      Analyzer analyzer = pluginRegistry.getBeanByFullPrefix(SENSEI_INDEX_ANALYZER, Analyzer.class);
      if (analyzer == null) {
        analyzer = new StandardAnalyzer(Version.LUCENE_35);
      }
      // Similarity from configuration:
      Similarity similarity = pluginRegistry.getBeanByFullPrefix(SENSEI_INDEX_SIMILARITY, Similarity.class);
      if (similarity == null) {
        similarity = new DefaultSimilarity();
      }
      ZoieConfig zoieConfig;
      if (_gateway != null){
        zoieConfig = new ZoieConfig(_gateway.getVersionComparator());
      }
      else{
        zoieConfig = new ZoieConfig();
      }
       
      zoieConfig.setAnalyzer(analyzer);
      zoieConfig.setSimilarity(similarity);
      zoieConfig.setBatchSize(_senseiConf.getInt(SENSEI_INDEX_BATCH_SIZE,ZoieConfig.DEFAULT_SETTING_BATCHSIZE));
      zoieConfig.setBatchDelay(_senseiConf.getLong(SENSEI_INDEX_BATCH_DELAY, ZoieConfig.DEFAULT_SETTING_BATCHDELAY));
      zoieConfig.setMaxBatchSize(_senseiConf.getInt(SENSEI_INDEX_BATCH_MAXSIZE, ZoieConfig.DEFAULT_MAX_BATCH_SIZE));
      zoieConfig.setRtIndexing(_senseiConf.getBoolean(SENSEI_INDEX_REALTIME, ZoieConfig.DEFAULT_SETTING_REALTIME));
      zoieConfig.setSkipBadRecord(_senseiConf.getBoolean(SENSEI_SKIP_BAD_RECORDS, false));
      int delay = _senseiConf.getInt(SENSEI_INDEX_FRESHNESS,10);
      ReaderCacheFactory readercachefactory;
      if (delay>0){
        readercachefactory = DefaultReaderCache.FACTORY;
        zoieConfig.setFreshness(delay*1000);
      }
      else{
        readercachefactory = SimpleReaderCache.FACTORY;
      }
      zoieConfig.setReadercachefactory(readercachefactory);
      ShardingStrategy strategy = pluginRegistry.getBeanByFullPrefix(SENSEI_SHARDING_STRATEGY, ShardingStrategy.class);
      if (strategy == null){
        strategy = new ShardingStrategy.FieldModShardingStrategy(_senseiSchema.getUidField());
      }
     
      pluggableSearchEngineManager = new PluggableSearchEngineManager();
      pluggableSearchEngineManager.init( _senseiConf.getString(SENSEI_INDEX_DIR), nodeid, _senseiSchema, zoieConfig.getVersionComparator(), pluginRegistry, strategy);      
      
      List<FacetHandler<?>> facetHandlers = new LinkedList<FacetHandler<?>>();
      List<RuntimeFacetHandlerFactory<?,?>> runtimeFacetHandlerFactories = new LinkedList<RuntimeFacetHandlerFactory<?,?>>();



    SenseiSystemInfo sysInfo = null;

      try {
        sysInfo = SenseiFacetHandlerBuilder.buildFacets(_schemaDoc, pluginRegistry, facetHandlers, runtimeFacetHandlerFactories, pluggableSearchEngineManager);
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
          String addr = NetUtil.getHostAddress();
          clusterInfo.add(new SenseiSystemInfo.SenseiNodeInfo(nodeid, partitions,
              String.format("%s:%d", addr, _senseiConf.getInt(SERVER_PORT)),
              String.format("http://%s:%d", addr, _senseiConf.getInt(SERVER_BROKER_PORT))));
          sysInfo.setClusterInfo(clusterInfo);
      }
      catch(Exception e)
      {
        throw new ConfigurationException(e.getMessage(), e);
      }
    }
    ZoieIndexableInterpreter interpreter =  pluginRegistry.getBeanByFullPrefix(SENSEI_INDEX_INTERPRETER, ZoieIndexableInterpreter.class);
    if (interpreter == null) {
      DefaultJsonSchemaInterpreter defaultInterpreter = new DefaultJsonSchemaInterpreter(_senseiSchema, pluggableSearchEngineManager);
      interpreter = defaultInterpreter;
      CustomIndexingPipeline customIndexingPipeline = pluginRegistry.getBeanByFullPrefix(SENSEI_INDEX_CUSTOM, CustomIndexingPipeline.class);
      if (customIndexingPipeline != null){
        try{
          defaultInterpreter.setCustomIndexingPipeline(customIndexingPipeline);
        }
        catch(Exception e){
          logger.error(e.getMessage(),e);
        }
      }
    }
    SenseiZoieFactory<?> zoieSystemFactory = constructZoieFactory(zoieConfig, facetHandlers, runtimeFacetHandlerFactories, interpreter);
    SenseiIndexingManager<?> indexingManager = pluginRegistry.getBeanByFullPrefix(SENSEI_INDEX_MANAGER, SenseiIndexingManager.class);

    
      if (indexingManager == null){
        indexingManager = new DefaultStreamingIndexingManager(_senseiSchema,_senseiConf, pluginRegistry, _gateway,strategy, pluggableSearchEngineManager);
      }
      SenseiQueryBuilderFactory queryBuilderFactory = pluginRegistry.getBeanByFullPrefix(SENSEI_QUERY_BUILDER_FACTORY, SenseiQueryBuilderFactory.class);
      if (queryBuilderFactory == null){
        QueryParser queryParser = new QueryParser(Version.LUCENE_35,"contents", analyzer);
        queryBuilderFactory = new DefaultJsonQueryBuilderFactory(queryParser);
      }
      SenseiCore senseiCore = new SenseiCore(nodeid,partitions,zoieSystemFactory,indexingManager,queryBuilderFactory, decorator);
      senseiCore.setSystemInfo(sysInfo);
    SenseiIndexPruner indexPruner = pluginRegistry.getBeanByFullPrefix(SENSEI_INDEX_PRUNER, SenseiIndexPruner.class);
    if (indexPruner != null){
      senseiCore.setIndexPruner(indexPruner);
    }
    if (pluggableSearchEngineManager != null) {
      senseiCore.setPluggableSearchEngineManager(pluggableSearchEngineManager);     
    }
    return senseiCore;
  }

  @SuppressWarnings("rawtypes")
  private SenseiZoieFactory<?> constructZoieFactory(ZoieConfig zoieConfig, List<FacetHandler<?>> facetHandlers,
                                                    List<RuntimeFacetHandlerFactory<?, ?>> runtimeFacetHandlerFactories, ZoieIndexableInterpreter interpreter)
      throws ConfigurationException {
    String indexerType = _senseiConf.getString(SENSEI_INDEXER_TYPE,"zoie");
     decorator = new SenseiIndexReaderDecorator(facetHandlers,runtimeFacetHandlerFactories);
    File idxDir = new File(_senseiConf.getString(SENSEI_INDEX_DIR));
    SenseiZoieFactory<?> zoieSystemFactory = null;

    DIRECTORY_MODE dirMode;
    String modeValue = _senseiConf.getString(SENSEI_INDEXER_MODE, "SIMPLE");
    if ("SIMPLE".equalsIgnoreCase(modeValue)){
      dirMode = DIRECTORY_MODE.SIMPLE;
    }
    else if ("NIO".equalsIgnoreCase(modeValue)){
      dirMode = DIRECTORY_MODE.NIO;
    }
    else if ("MMAP".equalsIgnoreCase(modeValue)){
      dirMode = DIRECTORY_MODE.MMAP;
    }
    else{
      logger.error("directory mode "+modeValue+" is not supported, SIMPLE is used.");
      dirMode = DIRECTORY_MODE.SIMPLE;
    }

    if (SENSEI_INDEXER_TYPE_ZOIE.equals(indexerType)){
      SenseiZoieSystemFactory senseiZoieFactory = new SenseiZoieSystemFactory(idxDir,dirMode,interpreter,decorator, zoieConfig);
      int retentionDays = _senseiConf.getInt(SENSEI_ZOIE_RETENTION_DAYS,-1);
      if (retentionDays>0){
        RetentionFilterFactory retentionFilterFactory = pluginRegistry.getBeanByFullPrefix(SENSEI_ZOIE_RETENTION_CLASS, RetentionFilterFactory.class);
        Filter purgeFilter = null;
        if (retentionFilterFactory!=null){
          purgeFilter = retentionFilterFactory.buildRetentionFilter(retentionDays);
        }
        else{
          String timeColumn = _senseiConf.getString(SENSEI_ZOIE_RETENTION_COLUMN, null);
          if (timeColumn==null){
            throw new ConfigurationException("Retention specified without a time column");
          }
          String unitString = _senseiConf.getString(SENSEI_ZOIE_RETENTION_TIMEUNIT,"seconds");
          TimeUnit unit = TimeUnit.valueOf(unitString.toUpperCase());
          if (unit == null){
            throw new ConfigurationException("Invalid timeunit for retention: "+unitString);
          }
          purgeFilter = new TimeRetentionFilter(timeColumn, retentionDays, unit);
        }
        if (purgeFilter != null && pluggableSearchEngineManager != null) {
          purgeFilter = new PurgeFilterWrapper(purgeFilter, pluggableSearchEngineManager);
        }
        senseiZoieFactory.setPurgeFilter(purgeFilter);
      }
      zoieSystemFactory = senseiZoieFactory;
    }
    else if (SENSEI_INDEXER_TYPE_HOURGLASS.equals(indexerType)) {
      String schedule = _senseiConf.getString(SENSEI_HOURGLASS_SCHEDULE,"");
      int trimThreshold = _senseiConf.getInt(SENSEI_HOURGLASS_TRIMTHRESHOLD,14);
      String frequencyString = _senseiConf.getString(SENSEI_HOURGLASS_FREQUENCY,"day");

      FREQUENCY frequency;

      if (SENSEI_HOURGLASS_FREQUENCY_MIN.equals(frequencyString)){
        frequency = FREQUENCY.MINUTELY;
      } else if (SENSEI_HOURGLASS_FREQUENCY_HOUR.equals(frequencyString)){
        frequency = FREQUENCY.HOURLY;
      }
      else if (SENSEI_HOURGLASS_FREQUENCY_DAY.equals(frequencyString)){
        frequency = FREQUENCY.DAILY;
      }
      else {
        throw new ConfigurationException("unsupported frequency setting: "+frequencyString);      }

      boolean appendOnly = _senseiConf.getBoolean(SENSEI_HOURGLASS_APPENDONLY, true);
      zoieSystemFactory = new SenseiHourglassFactory(idxDir,
                                                     dirMode,
                                                     interpreter,
                                                     decorator,
                                                     zoieConfig,
                                                     schedule,
                                                     appendOnly,
                                                     trimThreshold,
                                                     frequency,
                                                     pluggableSearchEngineManager != null ?
                                                       Arrays.asList(pluggableSearchEngineManager) :
                                                       Collections.EMPTY_LIST
                                                    );
    }  else{
      ZoieFactoryFactory zoieFactoryFactory= pluginRegistry.getBeanByFullPrefix(indexerType, ZoieFactoryFactory.class);
      if (zoieFactoryFactory==null){
        throw new ConfigurationException(indexerType+" not defined");
      }
      zoieSystemFactory = zoieFactoryFactory.getZoieFactory(idxDir, interpreter, decorator, zoieConfig);
    }
    String indexerCopier = _senseiConf.getString(SENSEI_INDEXER_COPIER);
    IndexCopier copier = pluginRegistry.getBeanByFullPrefix(SENSEI_INDEXER_COPIER, IndexCopier.class);
    if (copier != null) {
      zoieSystemFactory = new SenseiPairFactory(idxDir, dirMode,copier, interpreter, decorator, zoieConfig, zoieSystemFactory);
    }  else if (SENSEI_INDEXER_COPIER_HDFS.equals(indexerCopier))
    {
      zoieSystemFactory = new SenseiPairFactory(idxDir, dirMode,new HDFSIndexCopier(), interpreter, decorator, zoieConfig, zoieSystemFactory);
    } else
    {
      // do not support bootstrap index from other sources.

    }
    return zoieSystemFactory;
  }

  public Comparator<String> getVersionComparator() {
    return _gateway.getVersionComparator();
  }

  public SenseiServer buildServer() throws ConfigurationException {
    int port = _senseiConf.getInt(SERVER_PORT);
    JmxSenseiMBeanServer.registerCustomMBeanServer();

    ClusterClient clusterClient = buildClusterClient();

    NetworkServer networkServer = buildNetworkServer(_senseiConf,clusterClient);

    SenseiCore core = buildCore();

    List<AbstractSenseiCoreService<AbstractSenseiRequest, AbstractSenseiResult>> svcList = (List)pluginRegistry.resolveBeansByListKey(SENSEI_PLUGIN_SVCS, AbstractSenseiCoreService.class);


    return new SenseiServer(port,networkServer,clusterClient,core,svcList, pluginRegistry);

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
