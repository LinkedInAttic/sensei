package com.senseidb.indexing.activity;

import java.io.File;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import junit.framework.TestCase;

import org.easymock.classextension.EasyMock;
import org.json.JSONObject;

import proj.zoie.api.IndexReaderFactory;
import proj.zoie.api.Zoie;
import proj.zoie.api.ZoieIndexReader;
import proj.zoie.api.impl.DocIDMapperImpl;
import proj.zoie.impl.indexing.ZoieConfig;

import com.browseengine.bobo.api.BoboIndexReader;
import com.senseidb.conf.SenseiSchema.FieldDefinition;
import com.senseidb.search.node.SenseiCore;
import com.senseidb.test.SenseiStarter;

public class PurgeUnusedActivitiesJobTest extends TestCase {
  private File dir;
  private CompositeActivityValues compositeActivityValues;
  private Zoie zoie; 
  
  
  public void setUp() throws Exception {
    String pathname = getDirPath();
    SenseiStarter.rmrf(new File("sensei-test"));
    dir = new File(pathname);
    dir.mkdirs();
    
    ZoieIndexReader reader =  EasyMock.createMock(ZoieIndexReader.class);
    EasyMock.expect(reader.getDocIDMaper()).andReturn(new DocIDMapperImpl(new long[] {105L, 107L})).anyTimes();
    
     zoie = org.easymock.EasyMock.createMock(Zoie.class);
    org.easymock.EasyMock.expect(zoie.getIndexReaders()).andReturn(Arrays.asList(reader)).anyTimes();
    
    zoie.returnIndexReaders(org.easymock.EasyMock.<List>notNull());
    org.easymock.EasyMock.expectLastCall().anyTimes();
    org.easymock.EasyMock.replay(zoie);
    EasyMock.replay(reader);
    
  }
 
  public static String getDirPath() {
    return "sensei-test/activity2";
  }
  @Override
  protected void tearDown() throws Exception {
    compositeActivityValues.close();
    File file = new File("sensei-test");
    file.deleteOnExit();
    SenseiStarter.rmrf(file);
  }
  public void test1WriteValuesAndReadJustAfterThat() throws Exception {
    compositeActivityValues = CompositeActivityValues.createCompositeValues(ActivityPersistenceFactory.getInstance(getDirPath()), java.util.Arrays.asList(getLikesFieldDefinition() ), Collections.EMPTY_LIST, ZoieConfig.DEFAULT_VERSION_COMPARATOR);
    int valueCount = 100000;
    for (int i = 0; i < valueCount; i++) { 
      compositeActivityValues.update(i, String.format("%08d", i), ActivityPrimitiveValuesPersistenceTest.toMap(new JSONObject().put("likes", "+1")));
    }    
    compositeActivityValues.flush();
    compositeActivityValues.syncWithPersistentVersion(String.format("%08d", valueCount - 1));
    assertEquals(100000, compositeActivityValues.metadata.count);
    SenseiCore senseiCore = new SenseiCore(0, new int[] {0}, null, null, null, null) {
      @Override
      public IndexReaderFactory<ZoieIndexReader<BoboIndexReader>> getIndexReaderFactory(int partition) {
          return zoie;
      }
    };
    PurgeUnusedActivitiesJob purgeUnusedActivitiesJob = new PurgeUnusedActivitiesJob(compositeActivityValues, senseiCore, 1000L*1000);
    

    assertEquals(99498, purgeUnusedActivitiesJob.purgeUnusedActivityIndexes());
    compositeActivityValues.recentlyAddedUids.clear();
    assertEquals(500, purgeUnusedActivitiesJob.purgeUnusedActivityIndexes());
    assertEquals(2, compositeActivityValues.uidToArrayIndex.size());
    assertEquals(0, compositeActivityValues.deletedIndexes.size());
    assertEquals(100000, compositeActivityValues.metadata.count);
    compositeActivityValues.flush();
    Thread.sleep(2000);
    assertEquals(99998, compositeActivityValues.deletedIndexes.size());
    assertEquals(100000, compositeActivityValues.metadata.count);
    assertEquals(0, purgeUnusedActivitiesJob.purgeUnusedActivityIndexes());
    compositeActivityValues.flush();
    compositeActivityValues.executor.shutdown();
    assertEquals(100000, compositeActivityValues.metadata.count);
    compositeActivityValues.executor.awaitTermination(10, TimeUnit.SECONDS);
    for (int i = 0; i < 10; i++) { 
      compositeActivityValues.update(i, String.format("%08d", valueCount + i), ActivityPrimitiveValuesPersistenceTest.toMap(new JSONObject().put("likes", "+1")));
    }  
    assertEquals(12, compositeActivityValues.uidToArrayIndex.size());
    assertEquals(99988, compositeActivityValues.deletedIndexes.size());
    assertEquals(100000, compositeActivityValues.metadata.count);

  }
public static FieldDefinition getLikesFieldDefinition() {
    
    return getIntFieldDefinition("likes");
  }
  public static FieldDefinition getIntFieldDefinition(String name) {
    FieldDefinition fieldDefinition = new FieldDefinition();
    fieldDefinition.name = name;
    fieldDefinition.type = int.class;
    fieldDefinition.isActivity = true;
    return fieldDefinition;
  }
}
