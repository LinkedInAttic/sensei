package com.senseidb.search.query.filters;

import static org.easymock.classextension.EasyMock.*;

import com.browseengine.bobo.api.BoboIndexReader;
import com.browseengine.bobo.facets.FacetHandler;
import com.browseengine.bobo.facets.data.*;
import com.browseengine.bobo.facets.impl.MultiValueFacetHandler;
import com.browseengine.bobo.facets.impl.SimpleFacetHandler;
import com.senseidb.util.Pair;
import junit.framework.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Set;

public class TestSenseiTermFilter {

  private String[] vals = new String[]{"a", "c", "e"};
  private int[] freqs;
  TermValueList<String> dictionary = new TermStringList();

  @Before
  public void setup() {
    dictionary = new TermStringList();

    freqs = new int[27];
    dictionary.add(null);
    for(char ch = 'a'; ch <= 'z'; ch++) {
      dictionary.add("" + ch);
      freqs[1 + ch - 'a'] = 'z' - ch + 1;
    }
  }

  @Test
  public void testGetValsAndFreqsAndCardinality() {

    List<Pair<String,Integer>> valsAndFreqs = SenseiTermFilter.getValsAndFreqs(vals, dictionary, freqs);
    Assert.assertEquals("c", valsAndFreqs.get(1).getFirst());
    Assert.assertEquals(24, valsAndFreqs.get(1).getSecond().intValue());

    int andCardinality = SenseiTermFilter.estimateCardinality(valsAndFreqs, 26, true);
    int orCardinality = SenseiTermFilter.estimateCardinality(valsAndFreqs, 26, false);

    Assert.assertEquals(22, andCardinality);
    Assert.assertEquals(26, orCardinality);
  }

  @Test
  public void testSenseiTermFilter() throws IOException {
    String[] vals = new String[]{"a", "c", "e"};

    SenseiTermFilter orTermFilter =
        new SenseiTermFilter("column", vals, null, false, false);

    BoboIndexReader indexReader = createMock(BoboIndexReader.class);

    MultiValueFacetDataCache facetDataCache =
        new MultiValueFacetDataCache();
    facetDataCache.valArray = dictionary;
    facetDataCache.freqs = freqs;

    FacetHandler facetHandler =
        new MultiValueFacetHandler("column", 32);

    expect(indexReader.maxDoc()).andReturn(1000).anyTimes();
    expect(indexReader.getFacetHandler("column")).andReturn(facetHandler);
    expect(indexReader.getFacetData("column")).andReturn(facetDataCache).anyTimes();
    replay(indexReader);

    SenseiDocIdSet orDocIdSet = orTermFilter.getSenseiDocIdSet(indexReader);
    Assert.assertEquals(26 + 24 + 22, orDocIdSet.getCardinalityEstimate());

    SenseiTermFilter andTermFilter =
        new SenseiTermFilter("column", vals, null, true, false);

    reset(indexReader);
    expect(indexReader.maxDoc()).andReturn(1000).anyTimes();
    expect(indexReader.getFacetHandler("column")).andReturn(facetHandler);
    expect(indexReader.getFacetData("column")).andReturn(facetDataCache).anyTimes();
    replay(indexReader);

    SenseiDocIdSet andDocIdSet = andTermFilter.getSenseiDocIdSet(indexReader);
    Assert.assertEquals(22, andDocIdSet.getCardinalityEstimate());
  }
}
