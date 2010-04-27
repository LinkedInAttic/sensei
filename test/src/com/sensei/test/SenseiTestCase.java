package com.sensei.test;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import junit.framework.TestCase;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.util.Version;

import com.linkedin.norbert.network.javaapi.MessageHandler;
import com.sensei.search.nodes.NoOpIndexableInterpreter;
import com.sensei.search.nodes.SenseiNode;
import com.sensei.search.nodes.SenseiNodeMessageHandler;
import com.sensei.search.nodes.SenseiQueryBuilderFactory;
import com.sensei.search.nodes.SenseiSearchContext;
import com.sensei.search.nodes.impl.SimpleQueryBuilderFactory;
import com.sensei.search.req.SenseiRequest;
import com.sensei.search.req.SenseiResult;
import com.sensei.search.svc.impl.ClusteredSenseiServiceImpl;
import com.sensei.test.mocks.MockClientBootstrapFactory;
import com.sensei.test.mocks.MockServerBootstrapFactory;

public class SenseiTestCase extends TestCase {
	static File IdxDir = new File(System.getProperty("idx.dir"));
	static final String SENSEI_TEST_CLUSTER_NAME="testCluster";
	
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
		
		SenseiNode node1 = new SenseiNode(SENSEI_TEST_CLUSTER_NAME,1,1234,new MessageHandler[] {new SenseiNodeMessageHandler(srchCtx1)},"",
				new int[] {1,2});
		node1.setServerBootstrapFactory(new MockServerBootstrapFactory());
		SenseiNode node2 = new SenseiNode(SENSEI_TEST_CLUSTER_NAME,2,1232,new MessageHandler[] {new SenseiNodeMessageHandler(srchCtx2)},"",
				new int[] {2,3});
		node2.setServerBootstrapFactory(new MockServerBootstrapFactory());
		
		node1.startup(true);
		node2.startup(true);
		
		ClusteredSenseiServiceImpl clientSvc = new ClusteredSenseiServiceImpl(SENSEI_TEST_CLUSTER_NAME, "");
		clientSvc.setClientBootstrapFactory(new MockClientBootstrapFactory());
		clientSvc.startup();
		
		SenseiRequest req = new SenseiRequest();
		clientSvc.doQuery(req);
		SenseiResult res = clientSvc.doQuery(req);
		
		assertEquals(45000, res.getNumHits());
		clientSvc.shutdown();
		node1.shutdown();
		node2.shutdown();
	}
	
	public void testExt(){
		
	}
	
	public void testScatterGatherHandler(){
		
	}
	
	
}
