package com.senseidb.ba;

import java.io.File;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

import junit.framework.TestCase;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import org.junit.Test;

public class OfflineIndexTest extends TestCase {

  @Test
  public void test1() throws Exception {
    LineIterator lineIterator = FileUtils.lineIterator(new File(OfflineIndexTest.class.getClassLoader().getResource("data/test_data.json").toURI()));
    ArrayList<String> docs = new ArrayList<String>();
    while(lineIterator.hasNext()) {
      String car = lineIterator.next();
      if (car != null && car.contains("{"))
      docs.add(car);
    }
    Set<String> excludedColumns = new HashSet<String>();
    excludedColumns.add("tags");
    IndexSegment offlineSegment = IndexSegmentCreator.convert(docs.toArray(new String[docs.size()]), excludedColumns);
    assertEquals(15000, offlineSegment.getLength());
  }

}
