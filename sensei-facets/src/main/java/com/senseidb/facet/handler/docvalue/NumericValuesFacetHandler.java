package com.senseidb.facet.handler.docvalue;

import com.senseidb.facet.Facet;
import com.senseidb.facet.FacetIterator;
import com.senseidb.facet.FacetSelection;
import com.senseidb.facet.FacetSpec;
import com.senseidb.facet.data.BigSegmentedArray;
import com.senseidb.facet.handler.FacetCountCollector;
import com.senseidb.facet.handler.FacetCountCollectorSource;
import com.senseidb.facet.handler.FacetHandler;
import com.senseidb.facet.search.FacetAtomicReader;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.FieldComparatorSource;
import org.apache.lucene.search.Filter;
import org.apache.lucene.util.Bits;

import java.io.IOException;
import java.util.List;
import java.util.Set;

/**
 * @author Dmytro Ivchenko
 */
public class NumericValuesFacetHandler extends FacetHandler {
  public NumericValuesFacetHandler(String name, Set<String> dependsOn) {
    super(name, dependsOn);
  }

  public NumericValuesFacetHandler(String name) {
    this(name, null);
  }

  @Override
  public Filter buildFilter(final String value) throws IOException {
    return new Filter() {
      @Override
      public DocIdSet getDocIdSet(final AtomicReaderContext context, final Bits acceptDocs) throws IOException {
        return new DocIdSet() {
          @Override
          public DocIdSetIterator iterator() throws IOException {
            return context.reader().termDocsEnum(new Term(getName(), value));
          }
        };
      }
    };
  }

  @Override
  public FacetCountCollectorSource getFacetCountCollectorSource(final FacetSelection sel, final FacetSpec fspec) {
    return new FacetCountCollectorSource() {
      @Override
      public FacetCountCollector getFacetCountCollector(final FacetAtomicReader reader) {
        return new FacetCountCollector() {
          @Override
          public void collect(int docid) {
            //To change body of implemented methods use File | Settings | File Templates.
          }

          @Override
          public String getName() {
            return null;  //To change body of implemented methods use File | Settings | File Templates.
          }

          @Override
          public List<Facet> getTopFacets() {
            return null;  //To change body of implemented methods use File | Settings | File Templates.
          }

          @Override
          public Facet getFacet(String value) {
            return null;  //To change body of implemented methods use File | Settings | File Templates.
          }

          @Override
          public void close() {
            //To change body of implemented methods use File | Settings | File Templates.
          }

          @Override
          public FacetIterator iterator() {
            return null;  //To change body of implemented methods use File | Settings | File Templates.
          }
        };
      }
    };
  }

  @Override
  public String[] getFieldValues(FacetAtomicReader reader, int id) {
    return new String[0];  //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public FieldComparatorSource getFieldComparatorSource() {
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }
}
