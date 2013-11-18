package com.senseidb.search.query.filters;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class DocSetAssertions {
  static void assertRange(int min, int max, int maxDoc, DocIdSetCardinality c) {
    assertEquals("Lower bound", min, c.min * maxDoc, 1.0);
    assertEquals("Upper bound", max, c.max * maxDoc, 1.0);
  }

  static public void assertRange(double min, double max, DocIdSetCardinality c) {
    assertEquals("Lower bound", min, c.min, 0.0001);
    assertEquals("Upper bound", max, c.max, 0.0001);
  }
}
