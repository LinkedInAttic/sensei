package com.sensei.test;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import junit.framework.TestCase;

import org.apache.log4j.Logger;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.util.Version;

import com.linkedin.norbert.cluster.javaapi.ClusterClient;
import com.sensei.search.cluster.client.ClusterClientFactory;
import com.sensei.search.nodes.NoOpIndexableInterpreter;
import com.sensei.search.nodes.SenseiNode;
import com.sensei.search.nodes.SenseiNodeMessageHandler;
import com.sensei.search.nodes.SenseiQueryBuilderFactory;
import com.sensei.search.nodes.SenseiSearchContext;
import com.sensei.search.nodes.impl.SimpleQueryBuilderFactory;
import com.sensei.search.req.SenseiRequest;
import com.sensei.search.req.SenseiResult;
import com.sensei.search.svc.impl.ClusteredSenseiServiceImpl;

public class SenseiTestCase extends TestCase {
//	static File IdxDir = new File(System.getProperty("idx.dir"));
    static File IdxDir = new File("data/cardata");
	static final String SENSEI_TEST_CLUSTER_NAME="testCluster";
    private static final Logger logger = Logger.getLogger(SenseiTestCase.class);
    
	public SenseiTestCase(){
		super();
	}
	
	public SenseiTestCase(String testName){
		super(testName);
		
	}
	
	public void testHappyPath() throws Exception{
		QueryParser parser1 = new QueryParser(Version.LUCENE_CURRENT,"contents",new StandardAnalyzer(Version.LUCENE_CURRENT));
		QueryParser parser2 = new QueryParser(Version.LUCENE_CURRENT,"contents",new StandardAnalyzer(Version.LUCENE_CURRENT));
		
		HashMap<Integer,File> map1 = new HashMap<Integer,File>();
		System.out.println("idx.dir = " + IdxDir.toString());
		map1.put(1, IdxDir); 
		map1.put(2,IdxDir);
		
		Map<Integer,SenseiQueryBuilderFactory> qmap1 = new HashMap<Integer, SenseiQueryBuilderFactory>();
		qmap1.put(1, new SimpleQueryBuilderFactory(parser1));
		qmap1.put(2, new SimpleQueryBuilderFactory(parser1));

		HashMap<Integer,File> map2 = new HashMap<Integer,File>();
		map2.put(2, IdxDir);
		map2.put(3,IdxDir);
		
		Map<Integer,SenseiQueryBuilderFactory> qmap2 = new HashMap<Integer, SenseiQueryBuilderFactory>();
		qmap2.put(2, new SimpleQueryBuilderFactory(parser2));
		qmap2.put(3, new SimpleQueryBuilderFactory(parser2));

		SenseiSearchContext srchCtx1 = new SenseiSearchContext(qmap1, new NoOpIndexableInterpreter(), map1);
		SenseiSearchContext srchCtx2 = new SenseiSearchContext(qmap2, new NoOpIndexableInterpreter(), map2);
		
		// turn this into a factory class
		ClusterClient clusterClient = ClusterClientFactory.newInstance().newInMemoryClient(SENSEI_TEST_CLUSTER_NAME);
		
        ClusteredSenseiServiceImpl clientSvc = new ClusteredSenseiServiceImpl(SENSEI_TEST_CLUSTER_NAME, "", 30000, true);
        
        clientSvc.setClusterClient(clusterClient);
        clientSvc.startup();
        logger.info("Cluster client started");
		
		SenseiNode node1 = new SenseiNode(clusterClient, 1, 1233, new SenseiNodeMessageHandler(srchCtx1), new int[] {1,2});
		logger.info("Node 1 created with id : " + 1);
		SenseiNode node2 = new SenseiNode(clusterClient, 2, 1232, new SenseiNodeMessageHandler(srchCtx2), new int[] {2,3});
        logger.info("Node 2 created with id : " + 2);
        
		node1.startup(true);
        logger.info("Node 1 started");
		node2.startup(true);
        logger.info("Node 2 started");
				
		SenseiRequest req = new SenseiRequest();
		SenseiResult res = clientSvc.doQuery(req);
        logger.info("Query results received with numhits = " + res.getNumHits());
		
		assertEquals(45000, res.getNumHits());
		clientSvc.shutdown();
		logger.info("cluster client shutdown");
		node1.shutdown();
        logger.info("Node 1 shutdown");
		node2.shutdown();
        logger.info("Node 2 shutdown");
	}
	
	public void testExt(){
		
	}
	
	public void testScatterGatherHandler(){
		
	}
	
	
}
