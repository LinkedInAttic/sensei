/**
 * This software is licensed to you under the Apache License, Version 2.0 (the
 * "Apache License").
 *
 * LinkedIn's contributions are made under the Apache License. If you contribute
 * to the Software, the contributions will be deemed to have been made under the
 * Apache License, unless you expressly indicate otherwise. Please do not make any
 * contributions that would be inconsistent with the Apache License.
 *
 * You may obtain a copy of the Apache License at http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, this software
 * distributed under the Apache License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the Apache
 * License for the specific language governing permissions and limitations for the
 * software governed under the Apache License.
 *
 * © 2012 LinkedIn Corp. All Rights Reserved.  
 */

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
import com.senseidb.test.SenseiStarter;

public class PurgeUnusedActivitiesJobTest extends TestCase {
  private File dir;
  private Set<IndexReaderFactory<ZoieIndexReader<BoboIndexReader>>> zoieSystems;
  private CompositeActivityValues compositeActivityValues; 
  
  
  public void setUp() throws Exception {
    String pathname = getDirPath();
    SenseiStarter.rmrf(new File("sensei-test"));
    dir = new File(pathname);
    dir.mkdirs();
    zoieSystems = new HashSet<IndexReaderFactory<ZoieIndexReader<BoboIndexReader>>>();
    
    ZoieIndexReader reader =  EasyMock.createMock(ZoieIndexReader.class);
    EasyMock.expect(reader.getDocIDMaper()).andReturn(new DocIDMapperImpl(new long[] {105L, 107L})).anyTimes();
    
    Zoie zoie = org.easymock.EasyMock.createMock(Zoie.class);
    org.easymock.EasyMock.expect(zoie.getIndexReaders()).andReturn(Arrays.asList(reader)).anyTimes();
    
    zoie.returnIndexReaders(org.easymock.EasyMock.<List>notNull());
    org.easymock.EasyMock.expectLastCall().anyTimes();
    org.easymock.EasyMock.replay(zoie);
    EasyMock.replay(reader);
    zoieSystems.add(zoie);
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
    compositeActivityValues = CompositeActivityValues.createCompositeValues(ActivityPersistenceFactory.getInstance(getDirPath()), java.util.Arrays.asList(ActivityIntegrationTest.getLikesFieldDefinition() ), Collections.EMPTY_LIST, ZoieConfig.DEFAULT_VERSION_COMPARATOR);
    
    int valueCount = 10000;
    for (int i = 0; i < valueCount; i++) { 
      compositeActivityValues.update(i, String.format("%08d", i), ActivityPrimitiveValuesPersistenceTest.toMap(new JSONObject().put("likes", "+1")));
    }    
    compositeActivityValues.flush();
    compositeActivityValues.syncWithPersistentVersion(String.format("%08d", valueCount - 1));
    PurgeUnusedActivitiesJob purgeUnusedActivitiesJob = new PurgeUnusedActivitiesJob(compositeActivityValues, zoieSystems, 1000L*1000);
    
    assertEquals(9668, purgeUnusedActivitiesJob.purgeUnusedActivityIndexes());
    compositeActivityValues.recentlyAddedUids.clear();
    assertEquals(330, purgeUnusedActivitiesJob.purgeUnusedActivityIndexes());
    assertEquals(0, purgeUnusedActivitiesJob.purgeUnusedActivityIndexes());
    compositeActivityValues.flushDeletes();
    compositeActivityValues.executor.shutdown();
    compositeActivityValues.executor.awaitTermination(10, TimeUnit.SECONDS);
    for (int i = 0; i < 10; i++) { 
      compositeActivityValues.update(i, String.format("%08d", valueCount + i), ActivityPrimitiveValuesPersistenceTest.toMap(new JSONObject().put("likes", "+1")));
    }  
    assertEquals(12, compositeActivityValues.uidToArrayIndex.size());
    assertEquals(9988, compositeActivityValues.deletedIndexes.size());
  }

}
