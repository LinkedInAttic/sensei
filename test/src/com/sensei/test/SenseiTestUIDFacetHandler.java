package com.sensei.test;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import javax.management.MBeanServer;

import org.apache.log4j.Logger;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.util.Version;

import com.browseengine.bobo.api.BrowseHit;
import com.browseengine.bobo.api.BrowseSelection;
import com.linkedin.norbert.NorbertException;
import com.linkedin.norbert.cluster.ClusterShutdownException;
import com.sensei.search.nodes.NoOpIndexableInterpreter;
import com.sensei.search.nodes.SenseiBroker;
import com.sensei.search.nodes.SenseiNode;
import com.sensei.search.nodes.SenseiNodeMessageHandler;
import com.sensei.search.nodes.SenseiQueryBuilderFactory;
import com.sensei.search.nodes.SenseiSearchContext;
import com.sensei.search.nodes.impl.SimpleQueryBuilderFactory;
import com.sensei.search.req.SenseiRequest;
import com.sensei.search.req.SenseiResult;

public class SenseiTestUIDFacetHandler extends AbstractSenseiTestCase
{
  //static File IdxDir = new File(System.getProperty("uididx.dir"));
  //the index data has 10 docs with docIds from 0 to 9 and UIDs from 100 to 109
  static File IdxDir = new File("data/uiddata");
  static final String SENSEI_TEST_CLUSTER_NAME = "testCluster";
  private static final Logger logger = Logger.getLogger(SenseiTestUIDFacetHandler.class);

  private final MBeanServer mbeanServer = java.lang.management.ManagementFactory.getPlatformMBeanServer();

  public SenseiTestUIDFacetHandler()
  {
    super();
  }

  public SenseiTestUIDFacetHandler(String testName)
  {
    super(testName);
  }

  static SenseiBroker broker = null;
  static SenseiNode node1;
  static SenseiNode node2;

  static
  {
    QueryParser parser1 = new QueryParser(Version.LUCENE_CURRENT, "contents", new StandardAnalyzer(Version.LUCENE_CURRENT));
    QueryParser parser2 = new QueryParser(Version.LUCENE_CURRENT, "contents", new StandardAnalyzer(Version.LUCENE_CURRENT));

    HashMap<Integer, File> map1 = new HashMap<Integer, File>();
    logger.info("uididx.dir = " + IdxDir.toString());
    map1.put(1, IdxDir);
    map1.put(2, IdxDir);

    Map<Integer, SenseiQueryBuilderFactory> qmap1 = new HashMap<Integer, SenseiQueryBuilderFactory>();
    qmap1.put(1, new SimpleQueryBuilderFactory(parser1));
    qmap1.put(2, new SimpleQueryBuilderFactory(parser1));

    HashMap<Integer, File> map2 = new HashMap<Integer, File>();
    map2.put(2, IdxDir);
    map2.put(3, IdxDir);

    Map<Integer, SenseiQueryBuilderFactory> qmap2 = new HashMap<Integer, SenseiQueryBuilderFactory>();
    qmap2.put(2, new SimpleQueryBuilderFactory(parser2));
    qmap2.put(3, new SimpleQueryBuilderFactory(parser2));

    SenseiSearchContext srchCtx1 = new SenseiSearchContext(qmap1, new NoOpIndexableInterpreter(), map1);
    SenseiSearchContext srchCtx2 = new SenseiSearchContext(qmap2, new NoOpIndexableInterpreter(), map2);

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

    node1 = new SenseiNode(networkServer1, clusterClient, 1, 1233, new SenseiNodeMessageHandler(srchCtx1), new int[] { 1, 2 });
    logger.info("Node 1 created with id : " + 1);
    node2 = new SenseiNode(networkServer2, clusterClient, 2, 1232, new SenseiNodeMessageHandler(srchCtx2), new int[] { 2, 3 });
    logger.info("Node 2 created with id : " + 2);

    try
    {
      node1.startup(true);
    } catch (Exception e)
    {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    logger.info("Node 1 started");
    try
    {
      node2.startup(true);
    } catch (Exception e)
    {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    logger.info("Node 2 started");
  }

  public void testUIDFacetHandler() throws Exception
  {
    logger.info("executing test case testUIDFacetHandler");
    
    SenseiRequest req = new SenseiRequest();
    req.setCount(3);
    // UIDFacetHandler does not support facet counting and thus do not set facetSpec
   //  req.setFacetSpec("uid", facetSpec);
    
    BrowseSelection sel = new BrowseSelection("uid");
    String selVal = "104";
    sel.addValue(selVal);
    req.addSelection(sel);
    
    try
    {
      SenseiResult res = broker.browse(req);
      BrowseHit[] hits = res.getHits();
      
      for(int i = 0; i<hits.length; ++i)
      {
        String[] vals = hits[i].getFields("uid");
        
        assertEquals("104", vals[0]);
        int did = hits[i].getDocid();
        assertEquals(true, (did==4 || did==14 || did == 24));
      }
    }
    catch(Exception e)
    {
      e.printStackTrace();
    }
  }

}
