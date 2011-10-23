package com.sensei.test;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.log4j.Logger;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.DefaultSimilarity;
import org.apache.lucene.util.Version;

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

public class TestSensei extends AbstractSenseiTestCase
{
  static File ConfDir1 = new File("src/test/conf/node1");
  static File ConfDir2 = new File("src/test/conf/node2");

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
  ZoieSystem<BoboIndexReader,T> zoieSystem = new ZoieSystem<BoboIndexReader,T>(file,interpreter,new SenseiIndexReaderDecorator(),new StandardAnalyzer(Version.LUCENE_30),new DefaultSimilarity(),1000,300000,true,ZoieConfig.DEFAULT_VERSION_COMPARATOR);
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
  static SenseiServer node1;
  static SenseiServer node2;

  static
  {
    SenseiServerBuilder senseiServerBuilder1 = null;
    SenseiServerBuilder senseiServerBuilder2 = null;
    try
    {
      senseiServerBuilder1 = new SenseiServerBuilder(ConfDir1);
      node1 = senseiServerBuilder1.buildServer();
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

    logger.info("Cluster client started");

    Runtime.getRuntime().addShutdownHook(new Thread(){
        public void run(){
          try{
            broker.shutdown();
          }
          catch(Throwable t){}
          try{
            node1.shutdown();
          }
          catch(Throwable t){}
          try{
            node2.shutdown();
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
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    logger.info("Node 1 started");
    try
    {
      node2.start(true);
    } catch (Exception e)
    {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    logger.info("Node 2 started");

    try
    {
      //senseiServerBuilder1.buildHttpRestServer().start();

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
    logger.info("executing test case testGroupBy");
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
    logger.info("executing test case testGroupByVirtual");
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
    logger.info("executing test case testGroupBy");
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
