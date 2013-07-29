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

        return new SenseiDocIdSet(docIdSet, elems.length);
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
    expect(indexReader.maxDoc()).andReturn(1000);

    replay(indexReader);
    SenseiDocIdSet senseiDocIdSet = andFilter.getSenseiDocIdSet(indexReader);
    Assert.assertEquals(8, senseiDocIdSet.getCardinalityEstimate());
    Assert.assertEquals(7, getCount(senseiDocIdSet.getDocIdSet().iterator()));
  }

  @Test
  public void testOrFilter() throws IOException {
    List<SenseiFilter> filterList = getSenseiFilters();
    SenseiOrFilter filter = new SenseiOrFilter(filterList);

    IndexReader indexReader = createMock(IndexReader.class);
    expect(indexReader.maxDoc()).andReturn(1000);
    replay(indexReader);

    SenseiDocIdSet senseiDocIdSet = filter.getSenseiDocIdSet(indexReader);
    Assert.assertEquals(18, senseiDocIdSet.getCardinalityEstimate());
    Assert.assertEquals(11, getCount(senseiDocIdSet.getDocIdSet().iterator()));

    reset(indexReader);
    expect(indexReader.maxDoc()).andReturn(15);
    replay(indexReader);

    senseiDocIdSet = filter.getSenseiDocIdSet(indexReader);
    Assert.assertEquals(15, senseiDocIdSet.getCardinalityEstimate());
    Assert.assertEquals(11, getCount(senseiDocIdSet.getDocIdSet().iterator()));
  }

  @Test
  public void testNotFilter() throws IOException {
    List<SenseiFilter> filterList = getSenseiFilters();
    SenseiNotFilter filter = new SenseiNotFilter(new SenseiAndFilter(filterList));

    IndexReader indexReader = createMock(IndexReader.class);
    expect(indexReader.maxDoc()).andReturn(20).times(2);
    replay(indexReader);

    SenseiDocIdSet senseiDocIdSet = filter.getSenseiDocIdSet(indexReader);
    Assert.assertEquals(20, senseiDocIdSet.getCardinalityEstimate());
    Assert.assertEquals(13, getCount(senseiDocIdSet.getDocIdSet().iterator()));
  }


  private List<SenseiFilter> getSenseiFilters() {
    List<SenseiFilter> filterList = new ArrayList<SenseiFilter>();
    filterList.add(buildFilter(1, 3, 5, 7, 9, 11, 13, 15, 17, 19));
    filterList.add(buildFilter(2, 3, 5, 7, 11, 13, 17, 19));
    return filterList;
  }
}
