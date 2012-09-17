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

import com.senseidb.indexing.activity.primitives.ActivityIntValues;
import com.senseidb.indexing.activity.primitives.ActivityPrimitiveValues;
import com.senseidb.test.SenseiStarter;

import junit.framework.TestCase;

public class ActivityIntValuesIntTest extends TestCase {
  private File dir;

  public void setUp() {
    String pathname = getDirPath();
    SenseiStarter.rmrf(new File("sensei-test"));
    dir = new File(pathname);
    dir.mkdirs();
    
  }
  public static String getDirPath() {
    return "sensei-test/activity";
  }
  @Override
  protected void tearDown() throws Exception {
    File file = new File("sensei-test");
    file.deleteOnExit();
    SenseiStarter.rmrf(file);
  }
  
  public void test1() {
    ActivityIntValues intValues = (ActivityIntValues) ActivityPrimitiveValues.createActivityPrimitiveValues(ActivityPersistenceFactory.getInstance(getDirPath(), new ActivityConfig()), int.class, "likes", 0);
        
    long time = System.currentTimeMillis();
    for (int i = 0; i < 1000000; i++) {
      boolean update = intValues.update(i, "+1");
      if (update) {
        intValues.prepareFlush().run();
      }      
      if (i%1000000 == 0) {
        System.out.println(i);
      }
    }
    System.out.println("ElapsedTime = " + (System.currentTimeMillis() - time));
    intValues.close();
  }
}
