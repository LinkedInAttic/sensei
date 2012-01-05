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
import org.w3c.dom.Document;

import proj.zoie.api.IndexCopier;
import proj.zoie.api.indexing.ZoieIndexableInterpreter;
import proj.zoie.hourglass.impl.HourGlassScheduler.FREQUENCY;
import proj.zoie.impl.HDFSIndexCopier;
import proj.zoie.impl.indexing.ZoieConfig;
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
import com.sensei.indexing.api.ShardingStrategy;
import com.sensei.indexing.api.gateway.SenseiGateway;
import com.sensei.plugin.SenseiPluginRegistry;
import com.sensei.search.client.servlet.DefaultSenseiJSONServlet;
import com.sensei.search.client.servlet.SenseiConfigServletContextListener;
import com.sensei.search.client.servlet.SenseiHttpInvokerServiceServlet;
import com.sensei.search.cluster.routing.MD5HashProvider;
import com.sensei.search.cluster.routing.RingHashLoadBalancerFactory;
import com.sensei.search.cluster.routing.SenseiLoadBalancerFactory;
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
import com.sensei.search.query.TimeRetentionFilter;
import com.sensei.search.req.AbstractSenseiRequest;
import com.sensei.search.req.AbstractSenseiResult;
import com.sensei.search.req.SenseiSystemInfo;
import com.sensei.search.svc.impl.AbstractSenseiCoreService;

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
  private final Server _jettyServer;
  private final SenseiGateway _gateway;

  static final String SENSEI_CONTEXT_PATH = "sensei";




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
    senseiApp.setAttribute("sensei.search.version.comparator", _gateway.getVersionComparator());
    SenseiLoadBalancerFactory routerFactory = pluginRegistry.getBeanByFullPrefix(SERVER_SEARCH_ROUTER_FACTORY, SenseiLoadBalancerFactory.class);
    if (routerFactory == null) {
      routerFactory = new RingHashLoadBalancerFactory(new MD5HashProvider(), 1000);
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
      pluginRegistry = SenseiPluginRegistry.build(_senseiConf);
      pluginRegistry.start();
    }

    _gateway = constructGateway(_senseiConf);

    _schemaDoc = loadSchema(confDir);
    _senseiSchema = SenseiSchema.build(_schemaDoc);

    _jettyServer = buildHttpRestServer();
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

  private SenseiGateway constructGateway(Configuration conf) throws ConfigurationException{
      SenseiGateway gateway = pluginRegistry.getBeanByFullPrefix(SENSEI_GATEWAY, SenseiGateway.class);
      return gateway;

  }

  public SenseiCore buildCore() throws ConfigurationException {
    int nodeid = _senseiConf.getInt(NODE_ID);
    String partStr = _senseiConf.getString(PARTITIONS);
    String[] partitionArray = partStr.split("[,\\s]+");
    int[] partitions = buildPartitions(partitionArray);
    logger.info("partitions to serve: "+Arrays.toString(partitions));
  // Analyzer from configuration:
      Analyzer analyzer = pluginRegistry.getBeanByFullPrefix(SENSEI_INDEX_ANALYZER, Analyzer.class);
      if (analyzer == null) {
        analyzer = new StandardAnalyzer(Version.LUCENE_CURRENT);
      }
      // Similarity from configuration:
      Similarity similarity = pluginRegistry.getBeanByFullPrefix(SENSEI_INDEX_SIMILARITY, Similarity.class);
      if (similarity == null) {
        similarity = new DefaultSimilarity();
      }
      ZoieConfig zoieConfig = new ZoieConfig(_gateway.getVersionComparator());
      zoieConfig.setAnalyzer(analyzer);
      zoieConfig.setSimilarity(similarity);
      zoieConfig.setBatchSize(_senseiConf.getInt(SENSEI_INDEX_BATCH_SIZE,ZoieConfig.DEFAULT_SETTING_BATCHSIZE));
      zoieConfig.setBatchDelay(_senseiConf.getLong(SENSEI_INDEX_BATCH_DELAY, ZoieConfig.DEFAULT_SETTING_BATCHDELAY));
      zoieConfig.setMaxBatchSize(_senseiConf.getInt(SENSEI_INDEX_BATCH_MAXSIZE, ZoieConfig.DEFAULT_MAX_BATCH_SIZE));
      zoieConfig.setRtIndexing(_senseiConf.getBoolean(SENSEI_INDEX_REALTIME, ZoieConfig.DEFAULT_SETTING_REALTIME));
      zoieConfig.setFreshness(_senseiConf.getLong(SENSEI_INDEX_FRESHNESS, 500));
      zoieConfig.setSkipBadRecord(_senseiConf.getBoolean(SENSEI_SKIP_BAD_RECORDS, false));

      List<FacetHandler<?>> facetHandlers = new LinkedList<FacetHandler<?>>();
      List<RuntimeFacetHandlerFactory<?,?>> runtimeFacetHandlerFactories = new LinkedList<RuntimeFacetHandlerFactory<?,?>>();

      SenseiSystemInfo sysInfo = null;

      try {
        sysInfo = SenseiFacetHandlerBuilder.buildFacets(_schemaDoc, pluginRegistry, facetHandlers, runtimeFacetHandlerFactories);
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

      ZoieIndexableInterpreter interpreter =  pluginRegistry.getBeanByFullPrefix(SENSEI_INDEX_INTERPRETER, ZoieIndexableInterpreter.class);
      if (interpreter == null) {
        DefaultJsonSchemaInterpreter defaultInterpreter = new DefaultJsonSchemaInterpreter(_senseiSchema);
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
      
      ShardingStrategy strategy = pluginRegistry.getBeanByFullPrefix(SENSEI_SHARDING_STRATEGY, ShardingStrategy.class);
      if (strategy == null){
        strategy = new ShardingStrategy.FieldModShardingStrategy(_senseiSchema.getUidField());
      }

      if (indexingManager == null){
        indexingManager = new DefaultStreamingIndexingManager(_senseiSchema,_senseiConf, pluginRegistry, _gateway,strategy);
      }
      SenseiQueryBuilderFactory queryBuilderFactory = pluginRegistry.getBeanByFullPrefix(SENSEI_QUERY_BUILDER_FACTORY, SenseiQueryBuilderFactory.class);
      if (queryBuilderFactory == null){
        QueryParser queryParser = new QueryParser(Version.LUCENE_CURRENT,"contents", analyzer);
        queryBuilderFactory = new DefaultJsonQueryBuilderFactory(queryParser);
      }
      SenseiCore senseiCore = new SenseiCore(nodeid,partitions,zoieSystemFactory,indexingManager,queryBuilderFactory);
      senseiCore.setSystemInfo(sysInfo);

      SenseiIndexPruner indexPruner = pluginRegistry.getBeanByFullPrefix(SENSEI_INDEX_PRUNER, SenseiIndexPruner.class);
      if (indexPruner != null){
    	  senseiCore.setIndexPruner(indexPruner);
      }

      return senseiCore;
  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  private SenseiZoieFactory<?> constructZoieFactory(ZoieConfig zoieConfig, List<FacetHandler<?>> facetHandlers,
      List<RuntimeFacetHandlerFactory<?, ?>> runtimeFacetHandlerFactories, ZoieIndexableInterpreter interpreter)
      throws ConfigurationException {
    String indexerType = _senseiConf.getString(SENSEI_INDEXER_TYPE,"zoie");
    SenseiIndexReaderDecorator decorator = new SenseiIndexReaderDecorator(facetHandlers,runtimeFacetHandlerFactories);
    File idxDir = new File(_senseiConf.getString(SENSEI_INDEX_DIR));
    SenseiZoieFactory<?> zoieSystemFactory = null;
    if (SENSEI_INDEXER_TYPE_ZOIE.equals(indexerType)){
      SenseiZoieSystemFactory senseiZoieFactory = new SenseiZoieSystemFactory(idxDir,interpreter,decorator,
              zoieConfig);
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
      } else if (SENSEI_HOURGLASS_FREQUENCY_HOUR.equals(frequencyString)){
        frequency = FREQUENCY.HOURLY;
      }
      else if (SENSEI_HOURGLASS_FREQUENCY_DAY.equals(frequencyString)){
        frequency = FREQUENCY.DAILY;
      }
      else {
        throw new ConfigurationException("unsupported frequency setting: "+frequencyString);
      }
      zoieSystemFactory = new SenseiHourglassFactory(idxDir,interpreter,decorator,
            zoieConfig,schedule,trimThreshold,frequency);
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
      zoieSystemFactory = new SenseiPairFactory(idxDir, copier, interpreter, decorator, zoieConfig, zoieSystemFactory);
    }  else if (SENSEI_INDEXER_COPIER_HDFS.equals(indexerCopier))
    {
      zoieSystemFactory = new SenseiPairFactory(idxDir, new HDFSIndexCopier(), interpreter, decorator, zoieConfig, zoieSystemFactory);
    } else
    {
      // do not support bootstrap index from other sources.

    }
    return zoieSystemFactory;
  }

  public Server getJettyServer(){
    return _jettyServer;
  }

  public Comparator<String> getVersionComparator() {
    return _gateway.getVersionComparator();
  }

  public SenseiServer buildServer() throws ConfigurationException {
    int port = _senseiConf.getInt(SERVER_PORT);

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
