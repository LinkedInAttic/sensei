package com.sensei.test;

import java.io.InputStreamReader;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.OutputStreamWriter;
import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;
import java.net.URL;
import java.net.URLConnection;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.log4j.Logger;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.DefaultSimilarity;
import org.apache.lucene.util.Version;
import org.json.JSONObject;
import org.mortbay.jetty.Server;

import proj.zoie.api.IndexReaderFactory;
import proj.zoie.api.ZoieIndexReader;
import proj.zoie.api.indexing.ZoieIndexableInterpreter;
import proj.zoie.impl.indexing.ZoieConfig;
import proj.zoie.impl.indexing.ZoieSystem;

import com.browseengine.bobo.api.BoboIndexReader;
import com.browseengine.bobo.api.BrowseFacet;
import com.browseengine.bobo.api.BrowseSelection;
import com.browseengine.bobo.api.FacetAccessible;
import com.browseengine.bobo.api.FacetSpec;
import com.browseengine.bobo.api.FacetSpec.FacetSortSpec;
import com.browseengine.bobo.facets.data.FacetDataCache;
import com.browseengine.bobo.facets.data.FacetDataFetcher;
import com.linkedin.norbert.NorbertException;
import com.linkedin.norbert.cluster.ClusterShutdownException;
import com.linkedin.norbert.javacompat.cluster.ClusterClient;
import com.linkedin.norbert.javacompat.cluster.ZooKeeperClusterClient;
import com.linkedin.norbert.javacompat.network.NetworkClientConfig;
import com.sensei.conf.SenseiServerBuilder;
import com.sensei.search.cluster.client.SenseiNetworkClient;
import com.sensei.search.nodes.SenseiBroker;
import com.sensei.search.nodes.SenseiIndexReaderDecorator;
import com.sensei.search.nodes.SenseiQueryBuilderFactory;
import com.sensei.search.nodes.SenseiServer;
import com.sensei.search.nodes.impl.NoopIndexingManager;
import com.sensei.search.nodes.impl.SimpleQueryBuilderFactory;
import com.sensei.search.req.SenseiHit;
import com.sensei.search.req.SenseiRequest;
import com.sensei.search.req.SenseiResult;
import com.sensei.search.svc.api.SenseiService;
import com.sensei.search.svc.impl.HttpRestSenseiServiceImpl;

public class TestSensei extends AbstractSenseiTestCase
{
  static File ConfDir1 = new File("src/test/conf/node1");
  static File ConfDir2 = new File("src/test/conf/node2");
  static File IndexDir = new File("index/test");
  static URL SenseiUrl = null;

  private static final Logger logger = Logger.getLogger(TestSensei.class);

  public static FacetDataFetcher facetDataFetcher = new FacetDataFetcher()
  {
    public Object fetch(BoboIndexReader reader, int doc)
    {
      FacetDataCache dataCache = (FacetDataCache)reader.getFacetData("groupid");
      return dataCache.valArray.getRawValue(dataCache.orderArray.get(doc));
    }

    public void cleanup(BoboIndexReader reader)
    {
    }
  };

  public static FacetDataFetcher facetDataFetcherFixedLengthLongArray = new FacetDataFetcher()
  {
    private int counter = 0;

    public Object fetch(BoboIndexReader reader, int doc)
    {
      FacetDataCache dataCache = (FacetDataCache)reader.getFacetData("groupid");
      long[] val = new long[2];
      val[0] = counter%5;
      ++counter;
      Long groupId = (Long)dataCache.valArray.getRawValue(dataCache.orderArray.get(doc));
      if (groupId == null)
        val[1] = 0;
      else
        val[1] = groupId;
      return val;
    }

    public void cleanup(BoboIndexReader reader)
    {
      counter = 0;
    }
  };

  public TestSensei()
  {
    super();
  }

  public TestSensei(String testName)
  {
    super(testName);
  }

  
  public static <T> IndexReaderFactory<ZoieIndexReader<BoboIndexReader>> buildReaderFactory(File file,ZoieIndexableInterpreter<T> interpreter){
    ZoieConfig config = new ZoieConfig();
    config.setAnalyzer(new StandardAnalyzer(Version.LUCENE_34));
    config.setSimilarity(new DefaultSimilarity());
    config.setBatchSize(1000);
    config.setBatchDelay(300000);
    config.setRtIndexing(true);
    
    ZoieSystem<BoboIndexReader,T> zoieSystem = new ZoieSystem<BoboIndexReader,T>(file,interpreter,new SenseiIndexReaderDecorator(),config);
    //ZoieSystem<BoboIndexReader,T> zoieSystem = new ZoieSystem<BoboIndexReader,T>(file,interpreter,new SenseiIndexReaderDecorator(),new StandardAnalyzer(Version.LUCENE_34),new DefaultSimilarity(),1000,300000,true,ZoieConfig.DEFAULT_VERSION_COMPARATOR,false);
    zoieSystem.getAdminMBean().setFreshness(50);
    zoieSystem.start();
    return zoieSystem;
  }
  
  public static Map<Integer,IndexReaderFactory<ZoieIndexReader<BoboIndexReader>>> buildZoieFactoryMap(ZoieIndexableInterpreter<?> interpreter,Map<Integer,File> partFileMap){
    Map<Integer,IndexReaderFactory<ZoieIndexReader<BoboIndexReader>>> partReaderMap = new HashMap<Integer,IndexReaderFactory<ZoieIndexReader<BoboIndexReader>>>();
    Set<Entry<Integer,File>> entrySet = partFileMap.entrySet();
  
    for (Entry<Integer,File> entry : entrySet){
      partReaderMap.put(entry.getKey(), buildReaderFactory(entry.getValue(), interpreter));
    }
  
    return partReaderMap;
  }
  
  static SenseiBroker broker = null;
  static SenseiService httpRestSenseiService = null;
  static SenseiServer node1;
  static SenseiServer node2;
  static Server httpServer1;
  static Server httpServer2;

  static boolean rmrf(File f)
  {
    if (f != null)
    {
      if (f.isDirectory())
      {
        for (File sub : f.listFiles())
        {
          if (!rmrf(sub))
            return false;
        }
      }
      else
        return f.delete();
    }
    return true;
  }

  static JSONObject search(JSONObject req) throws Exception
  {
    URLConnection conn = SenseiUrl.openConnection();
    conn.setDoOutput(true);

    BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(conn.getOutputStream(), "UTF-8"));

    String reqStr = req.toString();
    System.out.println("req: " + reqStr);
    writer.write(reqStr, 0, reqStr.length());
    writer.flush();

    BufferedReader reader = new BufferedReader(new InputStreamReader(conn.getInputStream(), "UTF-8"));

    StringBuilder sb = new StringBuilder();
    String line = null;
    while((line = reader.readLine()) != null)
      sb.append(line);

    String res = sb.toString();
    System.out.println("res: " + res);

    return new JSONObject(res);
  }

  static
  {
    // Try to remove pre-existing test index files:
    try
    {
      SenseiUrl = new URL("http://localhost:8079/sensei");
    }
    catch (Exception e)
    {
      e.printStackTrace();
    }
    try
    {
      rmrf(IndexDir);
    }
    catch (Exception e)
    {
      // Ignore.
    }
    SenseiServerBuilder senseiServerBuilder1 = null;
    SenseiServerBuilder senseiServerBuilder2 = null;
    try
    {
      senseiServerBuilder1 = new SenseiServerBuilder(ConfDir1);
      node1 = senseiServerBuilder1.buildServer();
      httpServer1 = senseiServerBuilder1.buildHttpRestServer();
      logger.info("Node 1 created.");
    }
    catch (Exception e)
    {
      e.printStackTrace();
    }

    try
    {
      senseiServerBuilder2 = new SenseiServerBuilder(ConfDir2);
      node2 = senseiServerBuilder2.buildServer();
      httpServer2 = senseiServerBuilder2.buildHttpRestServer();
      logger.info("Node 2 created.");
    }
    catch (Exception e)
    {
      e.printStackTrace();
    }

    // register the request-response messages
    broker = null;
    try
    {
      broker = new SenseiBroker(networkClient, clusterClient, loadBalancerFactory);
      broker.setTimeoutMillis(0);
    } catch (NorbertException ne)
    {
      logger.info("shutting down cluster...", ne);
      try
      {
        clusterClient.shutdown();
      } catch (ClusterShutdownException e)
      {
        logger.info(e.getMessage(), e);
      } finally
      {
      }
    }

    httpRestSenseiService = new HttpRestSenseiServiceImpl("http", "localhost", 8079, "/sensei");

    logger.info("Cluster client started");

    Runtime.getRuntime().addShutdownHook(new Thread(){
        public void run(){
          try{
            broker.shutdown();
          }
          catch(Throwable t){}
          try{
            httpRestSenseiService.shutdown();
          }
          catch(Throwable t){}
          try{
            node1.shutdown();
          }
          catch(Throwable t){}
          try{
            httpServer1.stop();
          }
          catch(Throwable t){}
          try{
            node2.shutdown();
          }
          catch(Throwable t){}
          try{
            httpServer2.stop();
          }
          catch(Throwable t){}
          try{
            networkClient.shutdown();
          }
          catch(Throwable t){}
          try{
            clusterClient.shutdown();
          }
          catch(Throwable t){}
        }
      });

    try
    {
      node1.start(true);
    } catch (Exception e)
    {
      e.printStackTrace();
    }
    try
    {
      httpServer1.start();
    } catch (Exception e)
    {
      e.printStackTrace();
    }
    logger.info("Node 1 started");
    try
    {
      node2.start(true);
    } catch (Exception e)
    {
      e.printStackTrace();
    }
    try
    {
      httpServer2.start();
    } catch (Exception e)
    {
      e.printStackTrace();
    }
    logger.info("Node 2 started");

    try
    {
      SenseiRequest req = new SenseiRequest();
      SenseiResult res = null;
      int count = 0;
      do
      {
        Thread.sleep(5000);
        res = broker.browse(req);
        System.out.println(""+res.getNumHits()+" loaded...");
        ++count;
      } while (count < 20 && res.getNumHits() < 15000);
      // Thread.sleep(500000);
    } catch (Exception e)
    {
      e.printStackTrace();
    }
  }
  

  private void setspec(SenseiRequest req, FacetSpec spec)
  {
    req.setFacetSpec("color", spec);
    req.setFacetSpec("category", spec);
    req.setFacetSpec("city", spec);
    req.setFacetSpec("makemodel", spec);
    req.setFacetSpec("year", spec);
    req.setFacetSpec("price", spec);
    req.setFacetSpec("mileage", spec);
    req.setFacetSpec("tags", spec);
  }

  public void testTotalCount() throws Exception
  {
    logger.info("executing test case testTotalCount");
    SenseiRequest req = new SenseiRequest();
    SenseiResult res = broker.browse(req);
    assertEquals("wrong total number of hits" + req + res, 15000, res.getNumHits());
    logger.info("request:" + req + "\nresult:" + res);
  }

  public void testTotalCountWithFacetSpec() throws Exception
  {
    logger.info("executing test case testTotalCountWithFacetSpec");
    SenseiRequest req = new SenseiRequest();
    FacetSpec facetSpecall = new FacetSpec();
    facetSpecall.setMaxCount(1000000);
    facetSpecall.setExpandSelection(true);
    facetSpecall.setMinHitCount(0);
    facetSpecall.setOrderBy(FacetSortSpec.OrderHitsDesc);
    FacetSpec facetSpec = new FacetSpec();
    facetSpec.setMaxCount(5);
    setspec(req, facetSpec);
    req.setCount(5);
    setspec(req, facetSpecall);
    SenseiResult res = broker.browse(req);
    logger.info("request:" + req + "\nresult:" + res);
    verifyFacetCount(res, "year", "[1993 TO 1994]", 3090);
  }

  public void testSelection() throws Exception
  {
    logger.info("executing test case testSelection");
    FacetSpec facetSpecall = new FacetSpec();
    facetSpecall.setMaxCount(1000000);
    facetSpecall.setExpandSelection(true);
    facetSpecall.setMinHitCount(0);
    facetSpecall.setOrderBy(FacetSortSpec.OrderHitsDesc);
    FacetSpec facetSpec = new FacetSpec();
    facetSpec.setMaxCount(5);
    SenseiRequest req = new SenseiRequest();
    req.setCount(3);
    facetSpecall.setMaxCount(3);
    setspec(req, facetSpecall);
    BrowseSelection sel = new BrowseSelection("year");
    String selVal = "[2001 TO 2002]";
    sel.addValue(selVal);
    req.addSelection(sel);
    SenseiResult res = broker.browse(req);
    logger.info("request:" + req + "\nresult:" + res);
    assertEquals(2907, res.getNumHits());
    String selName = "year";
    verifyFacetCount(res, selName, selVal, 2907);
    verifyFacetCount(res, "year", "[1993 TO 1994]", 3090);
  }

  public void testSelectionNot() throws Exception
  {
    logger.info("executing test case testSelectionNot");
    FacetSpec facetSpecall = new FacetSpec();
    facetSpecall.setMaxCount(1000000);
    facetSpecall.setExpandSelection(true);
    facetSpecall.setMinHitCount(0);
    facetSpecall.setOrderBy(FacetSortSpec.OrderHitsDesc);
    FacetSpec facetSpec = new FacetSpec();
    facetSpec.setMaxCount(5);
    SenseiRequest req = new SenseiRequest();
    req.setCount(3);
    facetSpecall.setMaxCount(3);
    setspec(req, facetSpecall);
    BrowseSelection sel = new BrowseSelection("year");
    sel.addNotValue("[2001 TO 2002]");
    req.addSelection(sel);
    SenseiResult res = broker.browse(req);
    logger.info("request:" + req + "\nresult:" + res);
    assertEquals(12093, res.getNumHits());
    verifyFacetCount(res, "year", "[1993 TO 1994]", 3090);
  }

  public void testGroupBy() throws Exception
  {
    logger.info("executing test case testGroupBy");
    SenseiRequest req = new SenseiRequest();
    req.setCount(1);
    req.setGroupBy("groupid");
    SenseiResult res = broker.browse(req);
    logger.info("request:" + req + "\nresult:" + res);
    SenseiHit hit = res.getSenseiHits()[0];
    assertTrue(hit.getGroupHitsCount() > 0);
  }

  public void testGroupByWithGroupedHits() throws Exception
  {
    logger.info("executing test case testGroupBy");
    SenseiRequest req = new SenseiRequest();
    req.setCount(1);
    req.setGroupBy("groupid");
    req.setMaxPerGroup(8);
    SenseiResult res = broker.browse(req);
    logger.info("request:" + req + "\nresult:" + res);
    SenseiHit hit = res.getSenseiHits()[0];
    assertTrue(hit.getGroupHitsCount() > 0);
    assertTrue(hit.getSenseiGroupHits().length > 0);

    // use httpRestSenseiService
    res = httpRestSenseiService.doQuery(req);
    logger.info("request:" + req + "\nresult:" + res);
    hit = res.getSenseiHits()[0];
    assertTrue(hit.getGroupHitsCount() > 0);
    assertTrue(hit.getSenseiGroupHits().length > 0);
  }

  public void testGroupByVirtual() throws Exception
  {
    logger.info("executing test case testGroupByVirtual");
    SenseiRequest req = new SenseiRequest();
    req.setCount(1);
    req.setGroupBy("virtual_groupid");
    SenseiResult res = broker.browse(req);
    logger.info("request:" + req + "\nresult:" + res);
    SenseiHit hit = res.getSenseiHits()[0];
    assertTrue(hit.getGroupHitsCount() > 0);
  }

  public void testGroupByVirtualWithGroupedHits() throws Exception
  {
    logger.info("executing test case testGroupByVirtualWithGroupedHits");
    SenseiRequest req = new SenseiRequest();
    req.setCount(1);
    req.setGroupBy("virtual_groupid");
    req.setMaxPerGroup(8);
    SenseiResult res = broker.browse(req);
    logger.info("request:" + req + "\nresult:" + res);
    SenseiHit hit = res.getSenseiHits()[0];
    assertTrue(hit.getGroupHitsCount() > 0);
    assertTrue(hit.getSenseiGroupHits().length > 0);
  }

  public void testGroupByFixedLengthLongArray() throws Exception
  {
    logger.info("executing test case testGroupByFixedLengthLongArray");
    SenseiRequest req = new SenseiRequest();
    req.setCount(1);
    req.setGroupBy("virtual_groupid_fixedlengthlongarray");
    SenseiResult res = broker.browse(req);
    logger.info("request:" + req + "\nresult:" + res);
    SenseiHit hit = res.getSenseiHits()[0];
    assertTrue(hit.getGroupHitsCount() > 0);
  }

  public void testGroupByFixedLengthLongArrayWithGroupedHits() throws Exception
  {
    logger.info("executing test case testGroupByFixedLengthLongArrayWithGroupedHits");
    SenseiRequest req = new SenseiRequest();
    req.setCount(1);
    req.setGroupBy("virtual_groupid_fixedlengthlongarray");
    req.setMaxPerGroup(8);
    SenseiResult res = broker.browse(req);
    logger.info("request:" + req + "\nresult:" + res);
    SenseiHit hit = res.getSenseiHits()[0];
    assertTrue(hit.getGroupHitsCount() > 0);
    assertTrue(hit.getSenseiGroupHits().length > 0);
  }

  public void testSelectionTerm() throws Exception
  {
    logger.info("executing test case Selection term");
    String req = "{\"selections\":[{\"term\":{\"color\":{\"value\":\"red\"}}}]}";
    JSONObject res = search(new JSONObject(req));
    assertEquals("numhits is wrong", 2160, res.getInt("numhits"));
  }
  
  public void testSelectionTerms() throws Exception
  {
    logger.info("executing test case Selection terms");
    String req = "{\"selections\":[{\"terms\":{\"tags\":{\"values\":[\"mp3\",\"moon-roof\"],\"excludes\":[\"leather\"],\"operator\":\"or\"}}}]}";
    JSONObject res = search(new JSONObject(req));
    assertEquals("numhits is wrong", 4483, res.getInt("numhits"));
  }
  
  public void testSelectionRange() throws Exception
  {
    //2000 1548;
    //2001 1443;
    //2002 1464;
    // [2000 TO 2002]   ==> 4455
    // (2000 TO 2002)   ==> 1443
    // (2000 TO 2002]   ==> 2907
    // [2000 TO 2002)   ==> 2991
    {
      logger.info("executing test case Selection range [2000 TO 2002]");
      String req = "{\"selections\":[{\"range\":{\"year\":{\"to\":\"2002\",\"include_lower\":true,\"include_upper\":true,\"from\":\"2000\"}}}]}";
      JSONObject res = search(new JSONObject(req));
      assertEquals("numhits is wrong", 4455, res.getInt("numhits"));
    }
    
    {
      logger.info("executing test case Selection range (2000 TO 2002)");
      String req = "{\"selections\":[{\"range\":{\"year\":{\"to\":\"2002\",\"include_lower\":false,\"include_upper\":false,\"from\":\"2000\"}}}]}";
      JSONObject res = search(new JSONObject(req));
      assertEquals("numhits is wrong", 1443, res.getInt("numhits"));
    }
    
    {
      logger.info("executing test case Selection range (2000 TO 2002]");
      String req = "{\"selections\":[{\"range\":{\"year\":{\"to\":\"2002\",\"include_lower\":false,\"include_upper\":true,\"from\":\"2000\"}}}]}";
      JSONObject res = search(new JSONObject(req));
      assertEquals("numhits is wrong", 2907, res.getInt("numhits"));
    }
    
    {
      logger.info("executing test case Selection range [2000 TO 2002)");
      String req = "{\"selections\":[{\"range\":{\"year\":{\"to\":\"2002\",\"include_lower\":true,\"include_upper\":false,\"from\":\"2000\"}}}]}";
      JSONObject res = search(new JSONObject(req));
      assertEquals("numhits is wrong", 2991, res.getInt("numhits"));
    }
    
  }
  
  public void testMatchAllWithBoostQuery() throws Exception
  {
    logger.info("executing test case MatchAllQuery");
    String req = "{\"query\": {\"match_all\": {\"boost\": \"1.2\"}}}";
    JSONObject res = search(new JSONObject(req));
    assertEquals("numhits is wrong", 15000, res.getInt("numhits"));
  }
  
  public void testQueryStringQuery() throws Exception
  {
    logger.info("executing test case testQueryStringQuery");
    String req = "{\"query\": {\"query_string\": {\"query\": \"red AND cool\"}}}";
    JSONObject res = search(new JSONObject(req));
    assertEquals("numhits is wrong", 1070, res.getInt("numhits"));
  }

  public void testMatchAllQuery() throws Exception
  {
    logger.info("executing test case testMatchAllQuery");
    String req = "{\"query\": {\"match_all\": {}}}";
    JSONObject res = search(new JSONObject(req));
    assertEquals("numhits is wrong", 15000, res.getInt("numhits"));
  }

  public void testUIDQuery() throws Exception
  {
    logger.info("executing test case testUIDQuery");
    String req = "{\"query\": {\"ids\": {\"values\": [\"1\", \"2\", \"3\"], \"excludes\": [\"2\"]}}}";
    JSONObject res = search(new JSONObject(req));
    assertEquals("numhits is wrong", 2, res.getInt("numhits"));
    assertEquals("the first uid is wrong", 1, res.getJSONArray("hits").getJSONObject(0).getInt("uid"));
    assertEquals("the second uid is wrong", 3, res.getJSONArray("hits").getJSONObject(1).getInt("uid"));
  }

  public void testTextQuery() throws Exception
  {
    logger.info("executing test case testTextQuery");
    String req = "{\"query\": {\"text\": {\"contents\": { \"value\": \"red cool\", \"operator\": \"and\"}}}}";
    JSONObject res = search(new JSONObject(req));
    assertEquals("numhits is wrong", 1070, res.getInt("numhits"));
  }

  public void testTermQuery() throws Exception
  {
    logger.info("executing test case testTermQuery");
    String req = "{\"query\":{\"term\":{\"color\":\"red\"}}}";
    JSONObject res = search(new JSONObject(req));
    assertEquals("numhits is wrong", 2160, res.getInt("numhits"));
  }
  
  public void testBooleanQuery() throws Exception
  {
    logger.info("executing test case testBooleanQuery");
    String req = "{\"query\":{\"bool\":{\"must_not\":{\"term\":{\"category\":\"compact\"}},\"must\":{\"term\":{\"color\":\"red\"}}}}}";
    JSONObject res = search(new JSONObject(req));
    assertEquals("numhits is wrong", 1652, res.getInt("numhits"));
  }
  
  
  public void testDistMaxQuery() throws Exception
  {
    //color red ==> 2160
    //color blue ==> 1104
    logger.info("executing test case testDistMaxQuery");
    String req = "{\"query\":{\"dis_max\":{\"tie_breaker\":0.7,\"queries\":[{\"term\":{\"color\":\"red\"}},{\"term\":{\"color\":\"blue\"}}],\"boost\":1.2}}}";
    JSONObject res = search(new JSONObject(req));
    assertEquals("numhits is wrong", 3264, res.getInt("numhits"));
  }
  
  public void testPrefixQuery() throws Exception
  {
    //color blue ==> 1104
    logger.info("executing test case testPrefixQuery");
    String req = "{\"query\":{\"prefix\":{\"color\":{\"value\":\"blu\",\"boost\":2}}}}";
    JSONObject res = search(new JSONObject(req));
    assertEquals("numhits is wrong", 1104, res.getInt("numhits"));
  }
  
  
  public void testWildcardQuery() throws Exception
  {
    //color blue ==> 1104
    logger.info("executing test case testWildcardQuery");
    String req = "{\"query\":{\"wildcard\":{\"color\":{\"value\":\"bl*e\",\"boost\":2}}}}";
    JSONObject res = search(new JSONObject(req));
    assertEquals("numhits is wrong", 1104, res.getInt("numhits"));
  }
  
  
  
  /**
   * @param res
   *          result
   * @param selName
   *          the field name of the facet
   * @param selVal
   *          the value for which to check the count
   * @param count
   *          the expected count of the given value. If count>0, we verify the count. If count=0, it either has to NOT exist or it is 0.
   *          If count <0, it must not exist.
   */
  private void verifyFacetCount(SenseiResult res, String selName, String selVal, int count)
  {
    FacetAccessible year = res.getFacetAccessor(selName);
    List<BrowseFacet> browsefacets = year.getFacets();
    int index = indexOfFacet(selVal, browsefacets);
    if (count>0)
    {
    assertTrue("should contain a BrowseFacet for " + selVal, index >= 0);
    BrowseFacet bf = browsefacets.get(index);
    assertEquals(selVal + " has wrong count ", count, bf.getFacetValueHitCount());
    } else if (count == 0)
    {
      if (index >= 0)
      {
        // count has to be 0
        BrowseFacet bf = browsefacets.get(index);
        assertEquals(selVal + " has wrong count ", count, bf.getFacetValueHitCount());
      }
    } else
    {
      assertTrue("should not contain a BrowseFacet for " + selVal, index < 0);
    }
  }

  private int indexOfFacet(String selVal, List<BrowseFacet> browsefacets)
  {
    for (int i = 0; i < browsefacets.size(); i++)
    {
      if (browsefacets.get(i).getValue().equals(selVal))
        return i;
    }
    return -1;
  }

}
