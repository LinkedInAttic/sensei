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
