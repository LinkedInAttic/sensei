package com.senseidb.search.query.filters;

import com.kamikaze.docidset.impl.IntArrayDocIdSet;
import junit.framework.Assert;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.DocIdSetIterator;

import static org.easymock.classextension.EasyMock.*;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class TestSenseiBooleanFilters {

  public static SenseiFilter buildFilter(final int... elems) {
    return new SenseiFilter() {
      @Override
      public SenseiDocIdSet getSenseiDocIdSet(IndexReader reader) throws IOException {
        IntArrayDocIdSet docIdSet = new IntArrayDocIdSet(elems.length);
        for(int elem : elems) {
          docIdSet.addDoc(elem);
        }

        return new SenseiDocIdSet(docIdSet, DocIdSetCardinality.exact(elems.length, reader.maxDoc()), "IntArray[" + elems.length + "]");
      }
    };
  }

  public static int getCount(DocIdSetIterator iterator) throws IOException {
    int count = 0;
    while(iterator.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
      count++;
    }
    return count;
  }

  @Test
  public void testAndFilter() throws IOException {
    List<SenseiFilter> filterList = getSenseiFilters();
    SenseiAndFilter andFilter = new SenseiAndFilter(filterList);

    IndexReader indexReader = createMock(IndexReader.class);
    expect(indexReader.maxDoc()).andReturn(20).anyTimes();

    replay(indexReader);
    SenseiDocIdSet senseiDocIdSet = andFilter.getSenseiDocIdSet(indexReader);
    DocSetAssertions.assertRange(9, 14, 20, senseiDocIdSet.getCardinalityEstimate());
    Assert.assertEquals(14, getCount(senseiDocIdSet.getDocIdSet().iterator()));
  }

  @Test
  public void testOrFilter() throws IOException {
    List<SenseiFilter> filterList = getSenseiFilters();
    SenseiOrFilter filter = new SenseiOrFilter(filterList);

    IndexReader indexReader = createMock(IndexReader.class);
    expect(indexReader.maxDoc()).andReturn(20).anyTimes();
    replay(indexReader);

    SenseiDocIdSet senseiDocIdSet = filter.getSenseiDocIdSet(indexReader);
    DocSetAssertions.assertRange(15, 20, 20, senseiDocIdSet.getCardinalityEstimate());
    Assert.assertEquals(15, getCount(senseiDocIdSet.getDocIdSet().iterator()));
  }

  @Test
  public void testNotFilter() throws IOException {
    List<SenseiFilter> filterList = getSenseiFilters();
    SenseiNotFilter filter = new SenseiNotFilter(new SenseiAndFilter(filterList));

    IndexReader indexReader = createMock(IndexReader.class);
    expect(indexReader.maxDoc()).andReturn(20).anyTimes();
    replay(indexReader);

    SenseiDocIdSet senseiDocIdSet = filter.getSenseiDocIdSet(indexReader);
    DocSetAssertions.assertRange(6, 11, 20, senseiDocIdSet.getCardinalityEstimate());
    Assert.assertEquals(6, getCount(senseiDocIdSet.getDocIdSet().iterator()));
  }


  private List<SenseiFilter> getSenseiFilters() {
    List<SenseiFilter> filterList = new ArrayList<SenseiFilter>();
    filterList.add(buildFilter(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 13, 15, 17, 19)); // 15 elements
    filterList.add(buildFilter(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 13, 17, 19));     // 14 elements
    return filterList;
  }
}
