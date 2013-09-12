package com.senseidb.facet.handler.docvalue;

import com.senseidb.facet.iterator.FacetIterator;
import com.senseidb.facet.FacetSelection;
import com.senseidb.facet.FacetSpec;
import com.senseidb.facet.handler.FacetCountCollector;
import com.senseidb.facet.handler.FacetCountCollectorSource;
import com.senseidb.facet.handler.FacetHandler;
import com.senseidb.facet.iterator.NumericFacetIterator;
import com.senseidb.facet.search.FacetAtomicReader;
import it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongIterator;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.FieldComparatorSource;
import org.apache.lucene.search.Filter;
import org.apache.lucene.util.Bits;

import java.io.IOException;
import java.util.Arrays;
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
        try {
          return new NumericValuesFacetCountCollector(reader.getNumericDocValues(_name));
        } catch (IOException ex) {
          throw new RuntimeException(ex);
        }
      }
    };
  }

  private class NumericValuesFacetCountCollector implements FacetCountCollector {
    private final Long2IntOpenHashMap _values;
    private final NumericDocValues _docValues;

    public NumericValuesFacetCountCollector(NumericDocValues docValues) {
      _values = new Long2IntOpenHashMap();
      _values.defaultReturnValue(0);
      _docValues = docValues;
    }

    @Override
    public void collect(int docid) {
      long value = _docValues.get(docid);
      if (value > 0) {
        int count = _values.get(value) + 1;
        _values.put(value, count);
      }
    }

    @Override
    public void close() {
    }

    @Override
    public FacetIterator iterator() {
      return new NumericValuesFacetIterator(_values);
    }
  }

  private class NumericValuesFacetIterator extends NumericFacetIterator {
    private final long[] _sortedValues;
    private final Long2IntOpenHashMap _values;
    private int _currPos = 0;

    public NumericValuesFacetIterator(Long2IntOpenHashMap values) {
      _values = values;
      _sortedValues = new long[values.size()];
      LongIterator viter = values.keySet().iterator();
      int i = 0;
      while (viter.hasNext()) {
        _sortedValues[i++] = viter.nextLong();
      }

      Arrays.sort(_sortedValues);
    }

    @Override
    public long nextLong() {
      if (_currPos < _sortedValues.length) {
        long value = _sortedValues[_currPos++];
        _count = _values.get(value);
        return value;
      } else {
        _count = 0;
        return NumericFacetIterator.VALUE_MISSING;
      }
    }
  }

  @Override
  public String[] getFieldValues(FacetAtomicReader reader, int id) {
    try {
      NumericDocValues docValues = reader.getNumericDocValues(getName());
      long value = docValues.get(id);
      if (value > 0)
        return new String[] { String.valueOf(value) };
      else
        return new String[0];
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }
  }

  @Override
  public FieldComparatorSource getFieldComparatorSource() {
    return new FieldComparatorSource() {
      @Override
      public FieldComparator<?> newComparator(String fieldname, int numHits, int sortPos, boolean reversed) throws IOException {
        return new NumericValuesFieldComparator(numHits, reversed);
      }
    };
  }

  private class NumericValuesFieldComparator extends FieldComparator<Long> {
    private long[] _hits;
    private long _bottom;
    private int _reversed; // use int for better pipelining
    private NumericDocValues _docValues;

    public NumericValuesFieldComparator(int numHits, boolean reversed) {
      _hits = new long[numHits];
      _reversed = reversed ? -1 : 1;
    }

    @Override
    public int compare(int slot1, int slot2) {
      long diff = _hits[slot1] - _hits[slot2];
      return _reversed * (diff > 0 ? 1 : (diff < 0 ? -1 : 0));
    }

    @Override
    public void setBottom(int slot) {
      _bottom = _hits[slot];
    }

    @Override
    public int compareBottom(int doc) throws IOException {
      long diff = _bottom - _docValues.get(doc);
      return _reversed * (diff > 0 ? 1 : (diff < 0 ? -1 : 0));
    }

    @Override
    public void copy(int slot, int doc) throws IOException {
      _hits[slot] = doc;
    }

    @Override
    public FieldComparator<Long> setNextReader(AtomicReaderContext context) throws IOException {
      _docValues = context.reader().getNumericDocValues(getName());
      return this;
    }

    @Override
    public Long value(int slot) {
      return _hits[slot];
    }

    @Override
    public int compareDocToValue(int doc, Long value) throws IOException {
      long diff = _docValues.get(doc) - value;
      return _reversed * (diff > 0 ? 1 : (diff < 0 ? -1 : 0));
    }
  }
}
