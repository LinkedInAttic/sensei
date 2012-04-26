package com.senseidb.search.req;


import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.lucene.document.Document;
import org.apache.lucene.search.Explanation;

import com.linkedin.bobo.api.BrowseFacet;
import com.linkedin.bobo.api.BrowseHit;
import com.linkedin.bobo.api.BrowseResult;
import com.linkedin.bobo.api.FacetAccessible;
import com.senseidb.search.req.mapred.obsolete.SenseiMapReduceResult;


public class SenseiResult extends BrowseResult implements AbstractSenseiResult
{

  private static final long serialVersionUID = 1L;

  private String _parsedQuery = null;
 
  public SenseiHit[] getSenseiHits()
  {
    BrowseHit[] hits = getHits();
    if (hits == null || hits.length == 0)
    {
      return new SenseiHit[0];
    }
    return (SenseiHit[]) hits;
  }

  public void setParsedQuery(String query)
  {
    _parsedQuery = query;
  }

  public String getParsedQuery()
  {
    return _parsedQuery;
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof SenseiResult)) return false;
    SenseiResult b = (SenseiResult)o;

    if (!getParsedQuery().equals(b.getParsedQuery())) return false;

    // TODO: move this into BrowseResult equals
    if (!senseiHitsAreEqual(getSenseiHits(), b.getSenseiHits())) return false;
    if (getTid() != b.getTid()) return false;
    if (getTime() != b.getTime()) return false;
    if (getNumHits() != getNumHits()) return false;
    if (getNumGroups() != getNumGroups()) return false;
    if (getTotalDocs() != getTotalDocs()) return false;
    if (!facetMapsAreEqual(getFacetMap(), b.getFacetMap())) return false;

    return true;
  }

  private boolean senseiHitsAreEqual(SenseiHit[] a, SenseiHit[] b) {
    if (a == null) return b == null;
    if (a.length != b.length) return false;

    for (int i = 0; i < a.length; i++) {
      if (a[i].getUID() != b[i].getUID()) return false;
      if (a[i].getDocid() != b[i].getDocid()) return false;
      if (a[i].getScore() != b[i].getScore()) return false;
      if (a[i].getGroupValue() == null) {
        if (b[i].getGroupValue() != null) return false;
      }
      else {
        if (!a[i].getGroupValue().equals(b[i].getGroupValue())) return false;
      }
      if (a[i].getGroupHitsCount() != b[i].getGroupHitsCount()) return false;
      if (!senseiHitsAreEqual(a[i].getSenseiGroupHits(), b[i].getSenseiGroupHits())) return false;
      if (!expalanationsAreEqual(a[i].getExplanation(), b[i].getExplanation())) return false;

      // TODO: is comparing the document strings adequate?
      if (!storedFieldsAreEqual(a[i].getStoredFields(), b[i].getStoredFields())) return false;
// NOT YET SUPPORTED
//      if (!fieldValuesAreEqual(a[i].getFieldValues(), b[i].getFieldValues())) return false;
//      if (!rawFieldValuesAreEqual(a[i].getRawFieldValues(), b[i].getRawFieldValues())) return false;
    }

    return true;
  }

  private boolean storedFieldsAreEqual(Document a, Document b) {
    if (a == null) return b == null;
    return a.toString().equals(b.toString());
  }

  private boolean expalanationsAreEqual(Explanation a, Explanation b) {
    // TODO: is comparing the document strings adequate?
    return a.toString().equals(b.toString());
  }

  private boolean facetMapsAreEqual(Map<String, FacetAccessible> a, Map<String, FacetAccessible> b) {
    if (a == null) return b == null;
    if (a.size() != b.size()) return false;

    for (Entry<String,FacetAccessible> entry : a.entrySet()) {
      String fieldName = entry.getKey();
      if (!b.containsKey(fieldName)) return false;
      if (!facetAccessibleAreEqual(entry.getValue(), b.get(fieldName))) return false;
    }

    return true;
  }

  private boolean facetAccessibleAreEqual(FacetAccessible a, FacetAccessible b) {
    if (a == null) return b == null;
    if (a.getFacets().size() != b.getFacets().size()) return false;

    List<BrowseFacet> al = a.getFacets();
    List<BrowseFacet> bl = b.getFacets();

    if (!Arrays.equals(al.toArray(new BrowseFacet[al.size()]), bl.toArray(new BrowseFacet[bl.size()]))) return false;

    return true;
  }

  
   
}
