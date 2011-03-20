package com.sensei.conf;

import java.io.File;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.commons.beanutils.PropertyUtils;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.DefaultSimilarity;
import org.apache.lucene.search.Similarity;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.FileSystemXmlApplicationContext;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import com.linkedin.norbert.javacompat.cluster.ClusterClient;
import com.linkedin.norbert.javacompat.cluster.ZooKeeperClusterClient;
import com.linkedin.norbert.javacompat.network.NettyNetworkServer;
import com.linkedin.norbert.javacompat.network.NetworkServer;
import com.linkedin.norbert.javacompat.network.NetworkServerConfig;

import com.browseengine.bobo.facets.data.PredefinedTermListFactory;
import com.browseengine.bobo.facets.FacetHandler;
import com.browseengine.bobo.facets.impl.CompactMultiValueFacetHandler;
import com.browseengine.bobo.facets.impl.PathFacetHandler;
import com.browseengine.bobo.facets.impl.MultiValueFacetHandler;
import com.browseengine.bobo.facets.impl.RangeFacetHandler;
import com.browseengine.bobo.facets.impl.SimpleFacetHandler;
import com.sensei.search.nodes.impl.DefaultJsonQueryBuilderFactory;
import com.sensei.search.nodes.impl.DemoZoieSystemFactory;
import com.sensei.search.nodes.impl.NoopIndexLoaderFactory;
import com.sensei.search.nodes.NoOpIndexableInterpreter;
import com.sensei.search.nodes.SenseiIndexLoaderFactory;
import com.sensei.search.nodes.SenseiIndexReaderDecorator;
import com.sensei.search.nodes.SenseiQueryBuilderFactory;
import com.sensei.search.nodes.SenseiServer;
import com.sensei.search.nodes.SenseiZoieSystemFactory;

import proj.zoie.api.DefaultZoieVersion;
import proj.zoie.impl.indexing.ZoieConfig;

public class SenseiServerBuilder implements SenseiConfParams{

  public static final String SENSEI_PROPERTIES = "sensei.properties";
  public static final String CUSTOM_FACETS = "custom-facets.xml";
  public static final String SCHEMA_FILE = "schema.xml";
  public static final String PLUGINS = "plugins.xml";
  
  private final File _confDir;
  private final Configuration _senseiConf;
  private final ApplicationContext _pluginContext;
  private final ApplicationContext _customFacetContext;
  private final Document _schemaDoc;
  
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

  private PredefinedTermListFactory getPredefinedTermListFactory(String name) throws ConfigurationException {
    NodeList columns = _schemaDoc.getElementsByTagName("column");
    for (int i=0; i<columns.getLength(); ++i) {
      try {
        Element column = (Element)columns.item(i);
        String n = column.getAttributes().getNamedItem("name").getNodeValue();
        String t = column.getAttributes().getNamedItem("type").getNodeValue();
        String f = "";
        try {
          f = column.getAttributes().getNamedItem("format").getNodeValue();
        }
        catch(Exception ex) {
        }

        if (n.equals(name)) {
          if (t.equals("uid")) {
            return null;
          }
          else if (t.equals("int")) {
            if (f.isEmpty())
              return new PredefinedTermListFactory(Integer.TYPE, "00000000000000000000");
            else
              return new PredefinedTermListFactory(Integer.TYPE, f);
          }
          else if (t.equals("short")) {
            if (f.isEmpty())
              return new PredefinedTermListFactory(Short.TYPE, "00000000000000000000");
            else
              return new PredefinedTermListFactory(Short.TYPE, f);
          }
          else if (t.equals("char")) {
            if (f.isEmpty())
              return null;
            else
              return new PredefinedTermListFactory(Character.TYPE, f);
          }
          else if (t.equals("long")) {
            if (f.isEmpty())
              return new PredefinedTermListFactory(Long.TYPE, "00000000000000000000");
            else
              return new PredefinedTermListFactory(Long.TYPE, f);
          }
          else if (t.equals("float")) {
            if (f.isEmpty())
              return new PredefinedTermListFactory(Float.TYPE, "00000000000000000000");
            else
              return new PredefinedTermListFactory(Float.TYPE, f);
          }
          else if (t.equals("double")) {
            if (f.isEmpty())
              return new PredefinedTermListFactory(Double.TYPE, "00000000000000000000");
            else
              return new PredefinedTermListFactory(Double.TYPE, f);
          }
          else if (t.equals("string")) {
            if (f.isEmpty())
              return null;
            else
              return new PredefinedTermListFactory(String.class, f);
          }
          else if (t.equals("date")) {
            if (f.isEmpty())
              throw new Exception("Date format cannot be empty.");
            else
              return new PredefinedTermListFactory(Date.class, f);
          }
          else if (t.equals("text")) {
            if (f.isEmpty())
              return null;
            else
              return new PredefinedTermListFactory(String.class, f);
          }
          else {
            throw new Exception("Column type not supported: " + t);
          }
        }
      }
      catch(Exception e) {
        throw new ConfigurationException("Error parsing "+SCHEMA_FILE+": "+columns.item(i), e);
      }
    }
    return null;
  }

  private List buildFacets () throws ConfigurationException {
    NodeList facetsList = _schemaDoc.getElementsByTagName("facets");
    Element facetsEle = null;
    if (facetsList.getLength() > 0) {
      facetsEle = (Element)facetsList.item(0);
    }
    else {
      return Collections.EMPTY_LIST;
    }

    NodeList facetList = facetsEle.getElementsByTagName("facet");

    if (facetList.getLength() <= 0)
      return Collections.EMPTY_LIST;

    ArrayList facets = new ArrayList<FacetHandler>(facetList.getLength());

    for (int i=0; i<facetList.getLength(); ++i) {
      try {
        Element facet = (Element)facetList.item(i);
        String name = facet.getAttributes().getNamedItem("name").getNodeValue();
        String type = facet.getAttributes().getNamedItem("type").getNodeValue();
        Set<String> depends = new HashSet<String>();
        Node dependsNode = facet.getAttributes().getNamedItem("depends");
        if (dependsNode != null) {
          for (String dep : dependsNode.getNodeValue().split("[,; ]")) {
            dep = dep.trim();
            if (!dep.equals("")) {
              depends.add(dep);
            }
          }
        }

        FacetHandler facetHandler = null;
        List rangeList = new ArrayList<String>();
        if (type.equals("simple")) {
          facetHandler = new SimpleFacetHandler(name);
        }
        else if (type.equals("path")) {
          facetHandler = new PathFacetHandler(name);
        }
        else if (type.equals("range")) {
          // We need ranges before construct the RangeFacetHandler, skip at this moment.
        }
        else if (type.equals("multi")) {
          facetHandler = new MultiValueFacetHandler(name);
        }
        else if (type.equals("compact-multi")) {
          facetHandler = new CompactMultiValueFacetHandler(name);
        }
        else if (type.equals("custom")) {
          // Load from custom-facets spring configuration.
          facetHandler = (FacetHandler)_customFacetContext.getBean(name);
          facets.add(facetHandler);
          continue;
        }

        NodeList paramList = facet.getElementsByTagName("param");
        for (int j=0; j<paramList.getLength(); ++j) {
          Element param = (Element) paramList.item(j);
          String paramName = param.getAttributes().getNamedItem("name").getNodeValue();
          String paramValue = param.getAttributes().getNamedItem("value").getNodeValue();
          if (paramName.equals("range")) {
            if (!paramValue.matches("\\[.* TO .*\\]"))
              paramValue = "["+paramValue.replaceFirst("[-,]", " TO ")+"]";
            rangeList.add(paramValue);
          }
          else {
            // Set the bean properties.
            Class pType = PropertyUtils.getPropertyType(facetHandler, paramName);
            if (pType == null)  // No such properties.
              continue;
            Object objValue = paramValue;
            try {
              Constructor ctor = pType.getConstructor(String.class);
              objValue = ctor.newInstance(paramValue);
            }
            catch (NoSuchMethodException ex) {
            }
            PropertyUtils.setProperty(facetHandler, paramName, objValue);
          }
        }

        if (type.equals("range")) {
          facetHandler = new RangeFacetHandler(name, getPredefinedTermListFactory(name), rangeList);
        }
        facets.add(facetHandler);
      }
      catch (Exception e) {
        throw new ConfigurationException("Error parsing "+SCHEMA_FILE+": "+facetList.item(i), e);
      }
    }

    return facets;
  }
  
  public SenseiServerBuilder(File confDir) throws Exception{
	  _confDir = confDir;
	  File senseiConfFile = new File(confDir,SENSEI_PROPERTIES);
	  if (!senseiConfFile.exists()){
      throw new ConfigurationException("configuration file: "+senseiConfFile.getAbsolutePath()+" does not exist.");
	  }
	  _senseiConf = new PropertiesConfiguration(senseiConfFile);
	  _pluginContext = loadSpringContext(new File(confDir,PLUGINS));
	  _customFacetContext = loadSpringContext(new File(confDir,CUSTOM_FACETS));
	  
	  DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
	  dbf.setValidating(true);
	  dbf.setIgnoringComments(true);
	  DocumentBuilder db = dbf.newDocumentBuilder();
	  _schemaDoc = db.parse(new File(_confDir,SCHEMA_FILE));
	  _schemaDoc.getDocumentElement().normalize();
  }
  
  public SenseiServer buildServer() throws ConfigurationException {
	  int nodeid = _senseiConf.getInt(NODE_ID);
	  int port = _senseiConf.getInt(SERVER_PORT);
    String[] partitionArray = _senseiConf.getStringArray(PARTITIONS);
    int[] partitions = null;
    try {
      partitions = new int[partitionArray.length];
      for (int i=0; i<partitionArray.length; ++i) {
        partitions[i] = Integer.parseInt(partitionArray[i]);
      }
    }
    catch (Exception e) {
      throw new ConfigurationException(
        "Error parsing '" + SENSEI_PROPERTIES + "': " + PARTITIONS + "=" + _senseiConf.getString(PARTITIONS), e);
    }
	  
	  ClusterClient clusterClient = buildClusterClient(_senseiConf);
	  NetworkServer networkServer = buildNetworkServer(_senseiConf,clusterClient);
	  File extDir = new File(_confDir,"ext");

    // Analyzer from configuration:
    Analyzer analyzer = null;
    String analyzerName = _senseiConf.getString(SENSEI_INDEX_ANALYZER, "StandardAnalyzer");
    if (analyzerName == null || analyzerName.equals("StandardAnalyzer") || analyzerName.equals("")) {
      analyzer = new StandardAnalyzer();
    }
    else {
      analyzer = (Analyzer)_pluginContext.getBean(analyzerName);
    }

    // Similarity from configuration:
    Similarity similarity = null;
    String similarityName = _senseiConf.getString(SENSEI_INDEX_SIMILARITY, "DefaultSimilarity");
    if (similarityName == null || similarityName.equals("DefaultSimilarity") || similarityName.equals("")) {
      similarity = new DefaultSimilarity();
    }
    else {
      similarity = (Similarity)_pluginContext.getBean(similarityName);
    }

    ZoieConfig zoieConfig = new ZoieConfig(new DefaultZoieVersion.DefaultZoieVersionFactory());
    zoieConfig.setAnalyzer(analyzer);
    zoieConfig.setSimilarity(similarity);
    zoieConfig.setBatchSize(_senseiConf.getInt(SENSEI_INDEX_BATCH_SIZE, zoieConfig.getBatchSize()));
    zoieConfig.setBatchDelay(_senseiConf.getLong(SENSEI_INDEX_BATCH_DELAY, zoieConfig.getBatchDelay()));
    zoieConfig.setMaxBatchSize(_senseiConf.getInt(SENSEI_INDEX_BATCH_MAXSIZE, zoieConfig.getMaxBatchSize()));
    zoieConfig.setRtIndexing(_senseiConf.getBoolean(SENSEI_INDEX_REALTIME, zoieConfig.isRtIndexing()));
    zoieConfig.setFreshness(_senseiConf.getLong(SENSEI_INDEX_FRESHNESS, zoieConfig.getFreshness()));

    QueryParser queryParser = new QueryParser("contents", analyzer);

    SenseiZoieSystemFactory<?,?> zoieSystemFactory = new DemoZoieSystemFactory(
      new File(_senseiConf.getString(SENSEI_INDEX_DIR)),
      new NoOpIndexableInterpreter(),
      new SenseiIndexReaderDecorator(buildFacets(), Collections.EMPTY_LIST),
      zoieConfig
    );
    SenseiIndexLoaderFactory<?,?> indexLoaderFactory = new NoopIndexLoaderFactory();
    SenseiQueryBuilderFactory queryBuilderFactory = new DefaultJsonQueryBuilderFactory(queryParser);

	  return new SenseiServer(nodeid,
                            port,
                            partitions,
                            extDir,
                            networkServer,
                            clusterClient,
                            zoieSystemFactory,
                            indexLoaderFactory,
                            queryBuilderFactory);
  }
}
