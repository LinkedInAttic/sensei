package com.senseidb.search.query.filters;

import org.junit.Test;

public class TestDocIdSetCardinality {
  @Test
  public void testConstants() {
    DocIdSetCardinality c;
    DocSetAssertions.assertRange(0.0, 0.0, DocIdSetCardinality.zero());
    DocSetAssertions.assertRange(1.0, 1.0, DocIdSetCardinality.one());
    DocSetAssertions.assertRange(0.0, 1.0, DocIdSetCardinality.random());
    DocSetAssertions.assertRange(0.5, 0.5, DocIdSetCardinality.exact(.5));
    DocSetAssertions.assertRange(0.5, 0.5, DocIdSetCardinality.exact(5, 10));
  }

  @Test
  public void testAnds() {
    DocIdSetCardinality c;
    c = new DocIdSetCardinality(0.1, 0.9);
    c.andWith(new DocIdSetCardinality(0.1, 0.9));
    DocSetAssertions.assertRange(0, 0.9, c);

    c = new DocIdSetCardinality(0.8, 0.9);
    c.andWith(new DocIdSetCardinality(0.8, 0.9));
    DocSetAssertions.assertRange(0.6, 0.9, c);
  }

  @Test
  public void testOrs() {
    DocIdSetCardinality c;
    c = new DocIdSetCardinality(0.1, 0.2);
    c.orWith(new DocIdSetCardinality(0.1, 0.2));
    DocSetAssertions.assertRange(0.1, 0.4, c);

    c = new DocIdSetCardinality(0.8, 0.9);
    c.orWith(new DocIdSetCardinality(0.8, 0.9));
    DocSetAssertions.assertRange(0.8, 1.0, c);
  }

  @Test
  public void testNot() {
    DocIdSetCardinality c;
    c = new DocIdSetCardinality(0.1, 0.2);
    c.invert();
    DocSetAssertions.assertRange(0.8, 0.9, c);
  }

}
