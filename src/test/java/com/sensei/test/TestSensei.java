package com.sensei.test;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import javax.management.MBeanServer;

import org.apache.log4j.Logger;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.DefaultSimilarity;
import org.apache.lucene.util.Version;

import proj.zoie.api.DefaultZoieVersion;
import proj.zoie.api.IndexReaderFactory;
import proj.zoie.api.ZoieIndexReader;
import proj.zoie.api.DefaultZoieVersion.DefaultZoieVersionFactory;
import proj.zoie.api.indexing.ZoieIndexableInterpreter;
import proj.zoie.impl.indexing.ZoieSystem;

import com.browseengine.bobo.api.BoboIndexReader;
import com.browseengine.bobo.api.BrowseFacet;
import com.browseengine.bobo.api.BrowseSelection;
import com.browseengine.bobo.api.FacetAccessible;
import com.browseengine.bobo.api.FacetSpec;
import com.browseengine.bobo.api.FacetSpec.FacetSortSpec;
import com.linkedin.norbert.NorbertException;
import com.linkedin.norbert.cluster.ClusterShutdownException;
import com.sensei.search.nodes.SenseiBroker;
import com.sensei.search.nodes.SenseiIndexReaderDecorator;
import com.sensei.search.nodes.SenseiQueryBuilderFactory;
import com.sensei.search.nodes.SenseiServer;
import com.sensei.search.nodes.impl.NoopIndexingManager;
import com.sensei.search.nodes.impl.SimpleQueryBuilderFactory;
import com.sensei.search.req.SenseiRequest;
import com.sensei.search.req.SenseiResult;

public class TestSensei extends AbstractSenseiTestCase
{
  // static File IdxDir = new File(System.getProperty("idx.dir"));
  static File IdxDir = new File("data/cardata");
  static final String SENSEI_TEST_CLUSTER_NAME = "testCluster";
  private static final Logger logger = Logger.getLogger(TestSensei.class);

  private final MBeanServer mbeanServer = java.lang.management.ManagementFactory.getPlatformMBeanServer();

  public TestSensei()
  {
    super();
  }

  public TestSensei(String testName)
  {
    super(testName);
  }

  
  public static <T> IndexReaderFactory<ZoieIndexReader<BoboIndexReader>> buildReaderFactory(File file,ZoieIndexableInterpreter<T> interpreter){
	ZoieSystem<BoboIndexReader,T,DefaultZoieVersion> zoieSystem = new ZoieSystem<BoboIndexReader,T,DefaultZoieVersion>(file,interpreter,new SenseiIndexReaderDecorator(),new StandardAnalyzer(Version.LUCENE_CURRENT),new DefaultSimilarity(),1000,300000,true,new DefaultZoieVersionFactory());
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
    QueryParser parser1 = new QueryParser(Version.LUCENE_CURRENT, "contents", new StandardAnalyzer(Version.LUCENE_CURRENT));
    QueryParser parser2 = new QueryParser(Version.LUCENE_CURRENT, "contents", new StandardAnalyzer(Version.LUCENE_CURRENT));

    HashMap<Integer, File> map1 = new HashMap<Integer, File>();
    logger.info("idx.dir = " + IdxDir.toString());
    map1.put(1, IdxDir);
    map1.put(2, IdxDir);

    Map<Integer, SenseiQueryBuilderFactory> qmap1 = new HashMap<Integer, SenseiQueryBuilderFactory>();
    qmap1.put(1, new SimpleQueryBuilderFactory(parser1));
    qmap1.put(2, new SimpleQueryBuilderFactory(parser1));

    HashMap<Integer, File> map2 = new HashMap<Integer, File>();
    //map2.put(2, IdxDir);
    map2.put(3, IdxDir);

    Map<Integer, SenseiQueryBuilderFactory> qmap2 = new HashMap<Integer, SenseiQueryBuilderFactory>();
    //qmap2.put(2, new SimpleQueryBuilderFactory(parser2));
    qmap2.put(3, new SimpleQueryBuilderFactory(parser2));

    // register the request-response messages
    broker = null;
    try
    {
      broker = new SenseiBroker(networkClient, clusterClient, requestRewriter, routerFactory);
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

    node1 = new SenseiServer(1,1233,new int[] { 1, 2 },null,networkServer1, clusterClient,_zoieFactory,new NoopIndexingManager(), new SimpleQueryBuilderFactory(parser1),null);
    logger.info("Node 1 created with id : " + 1);
    node2 = new SenseiServer(2,1332,new int[] {3},null,networkServer2, clusterClient,_zoieFactory,new NoopIndexingManager(), new SimpleQueryBuilderFactory(parser2),null);
    logger.info("Node 2 created with id : " + 2);

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
      Thread.sleep(1000);
    } catch (InterruptedException e)
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
    assertEquals("wrong total number of hits" + req + res, 45000, res.getNumHits());
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
    verifyFacetCount(res, "year", "[1993 TO 1994]", 9270);
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
    assertEquals(3 * 2907, res.getNumHits());
    String selName = "year";
    verifyFacetCount(res, selName, selVal, 3 * 2907);
    verifyFacetCount(res, "year", "[1993 TO 1994]", 9270);
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
    assertEquals(res.getTotalDocs() - 3 * 2907, res.getNumHits());
    verifyFacetCount(res, "year", "[1993 TO 1994]", 9270);
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
