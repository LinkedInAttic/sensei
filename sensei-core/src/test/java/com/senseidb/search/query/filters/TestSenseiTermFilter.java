package com.senseidb.search.query.filters;

import static org.easymock.classextension.EasyMock.*;
import static org.junit.Assert.*;

import com.browseengine.bobo.api.BoboIndexReader;
import com.browseengine.bobo.facets.FacetHandler;
import com.browseengine.bobo.facets.data.*;
import com.browseengine.bobo.facets.impl.MultiValueFacetHandler;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

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
  public void testGetValsByFrequency() {

    DocIdSetCardinality andCardinality = DocIdSetCardinality.one();
    String andVals[] = SenseiTermFilter.getValsByFrequency(vals, freqs, 26, andCardinality, dictionary, true);
    assertArrayEquals(andVals, new String[]{"e", "c", "a"});
    DocSetAssertions.assertRange(20, 22, 26, andCardinality);

    DocIdSetCardinality orCardinality = DocIdSetCardinality.zero();
    String orgVals[] = SenseiTermFilter.getValsByFrequency(vals, freqs, 26, orCardinality, dictionary, false);
    assertArrayEquals(orgVals, new String[]{"a", "c", "e"});
    DocSetAssertions.assertRange(26, 26, 26, orCardinality);
  }

  @Test
  public void testSenseiTermFilter() throws IOException {
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
    DocSetAssertions.assertRange(26, 72, 1000, orDocIdSet.getCardinalityEstimate());

    SenseiTermFilter andTermFilter =
        new SenseiTermFilter("column", vals, null, true, false);

    reset(indexReader);
    expect(indexReader.maxDoc()).andReturn(1000).anyTimes();
    expect(indexReader.getFacetHandler("column")).andReturn(facetHandler);
    expect(indexReader.getFacetData("column")).andReturn(facetDataCache).anyTimes();
    replay(indexReader);

    SenseiDocIdSet andDocIdSet = andTermFilter.getSenseiDocIdSet(indexReader);
    DocSetAssertions.assertRange(0, 22, 1000, andDocIdSet.getCardinalityEstimate());
  }
}
