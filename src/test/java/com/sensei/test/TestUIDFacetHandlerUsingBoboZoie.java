package com.sensei.test;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import javax.management.MBeanServer;

import junit.framework.TestCase;

import org.apache.log4j.Logger;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.store.FSDirectory;

import proj.zoie.api.ZoieIndexReader;
import proj.zoie.api.ZoieMultiReader;

import com.browseengine.bobo.api.BoboBrowser;
import com.browseengine.bobo.api.BoboIndexReader;
import com.browseengine.bobo.api.BrowseHit;
import com.browseengine.bobo.api.BrowseRequest;
import com.browseengine.bobo.api.BrowseResult;
import com.browseengine.bobo.api.BrowseSelection;
import com.browseengine.bobo.api.MultiBoboBrowser;
import com.browseengine.bobo.facets.FacetHandler;
import com.sensei.search.facet.UIDFacetHandler;
import com.sensei.search.nodes.SenseiIndexReaderDecorator;

public class TestUIDFacetHandlerUsingBoboZoie extends TestCase
{
  // the index data has 10 docs with docIds from 0 to 9 and UIDs from 100 to 109
  static File IdxDir = new File("data/uiddata");
  private static final Logger logger = Logger.getLogger(TestUIDFacetHandlerUsingBoboZoie.class);
  private final MBeanServer mbeanServer = java.lang.management.ManagementFactory.getPlatformMBeanServer();

  public TestUIDFacetHandlerUsingBoboZoie()
  {
    super();
  }

  public TestUIDFacetHandlerUsingBoboZoie(String testName)
  {
    super(testName);
  }

  public void testNewIndexData() throws Exception
  {
    logger.info("executing test case testNewIndexData");
    List<FacetHandler<?>> facetHandlers = new ArrayList<FacetHandler<?>>();
    facetHandlers.add(new UIDFacetHandler("uid"));

    IndexReader indexReader = IndexReader.open(FSDirectory.open(IdxDir));
    SenseiIndexReaderDecorator senseiIndexReaderDecorator = new SenseiIndexReaderDecorator(facetHandlers, null);
    ZoieIndexReader zoieIndexReader = new ZoieMultiReader(indexReader, senseiIndexReaderDecorator);
    List<BoboIndexReader> boboReaderList = zoieIndexReader.getDecoratedReaders();

    //    for(BoboIndexReader bReader : boboReaderList)
    //    {
    //      IndexReader innerReader = bReader.getInnerReader();
    //      if (innerReader instanceof ZoieSegmentReader)
    //      {
    //        ZoieSegmentReader zReader = (ZoieSegmentReader)innerReader;
    //        if(zReader != null)
    //        {
    //          long[] uids =  zReader.getUIDArray();
    //        }
    //      }
    //    }

    BrowseRequest br=new BrowseRequest();
    br.setOffset(0);
    br.setCount(3); 

    BrowseSelection uidSel=new BrowseSelection("uid");
    uidSel.addValue("104");
    br.addSelection(uidSel);

    MultiBoboBrowser browser = null; 
    try{
      browser = new MultiBoboBrowser(BoboBrowser.createBrowsables(boboReaderList));

      BrowseResult res=browser.browse(br);
      BrowseHit[] hits = res.getHits();
      String[] vals = hits[0].getFields("uid");
      assertEquals(4, hits[0].getDocid());
      assertEquals("104", vals[0]);
    }
    catch(Exception e)
    {
      e.printStackTrace();
    }
    finally{
      try{
        if (browser!=null){
          browser.close();
        }
      }
      finally{
        if (zoieIndexReader!=null){
          zoieIndexReader.close();
        }
      }
    }
  }
}

