package com.senseidb.facet.search;


import com.senseidb.facet.FacetRequestParams;
import com.senseidb.facet.FacetRequest;
import com.senseidb.facet.FacetSelection;
import com.senseidb.facet.FacetAccessible;
import com.senseidb.facet.FacetSpec;
import com.senseidb.facet.handler.CombinedFacetAccessible;
import com.senseidb.facet.handler.FacetCountCollector;
import com.senseidb.facet.handler.FacetHandler;
import com.senseidb.facet.handler.RuntimeFacetHandler;
import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.Scorer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;


/**
 * @author Dmytro Ivchenko
 */
public class FacetCollector extends Collector {

  private final FacetRequest _request;
  private final FacetRequestParams _requestParams;
  private final Collector _innerCollector;

  private Scorer _scorer;
  private AtomicReaderContext _context;
  private List<FacetContext> _facetContexts;
  private FacetValidator _validator;
  private Map<String, Map<AtomicReader, FacetAccessible>> _facetCollectors;

  public FacetCollector(Collector collector, FacetRequest request) {
    _innerCollector = collector;
    _request = request;
    _requestParams = request.getParams();
    _facetCollectors = new HashMap<String, Map<AtomicReader, FacetAccessible>>();
  }

  @Override
  public void setScorer(Scorer scorer)
      throws IOException {
    _innerCollector.setScorer(_scorer);

    _scorer = scorer;
  }

  @Override
  public void collect(int doc)
      throws IOException {
    if (_validator.validate(doc)) {
      _innerCollector.collect(doc);
    }
  }

  @Override
  public void setNextReader(AtomicReaderContext context)
      throws IOException {
    updateFacetCollectors();

    _innerCollector.setNextReader(context);

    _context = context;

    for (Map.Entry<String, RuntimeFacetHandler<?>> entry : _request.getRuntimeFacetHandlerMap().entrySet()) {
      try {
        // Load data
        Object data = entry.getValue().load((FacetAtomicReader) _context.reader());
        entry.getValue().putFacetData((FacetAtomicReader) _context.reader(), data);
      } catch (IOException ex) {
        throw new IOException("Error trying to load FacetHandler : " + entry.getKey(), ex);
      }
    }

    _facetContexts = new LinkedList<FacetContext>();
    for (FacetHandler<?> facetHandler : _request.getAllFacetHandlerMap().values()) {
      String name = facetHandler.getName();

      FacetSelection sel = _requestParams.getSelection(name);
      Filter filter = null;
      if (sel != null) {
        filter = facetHandler.buildFilter(sel);
      }

      FacetSpec fspec = _requestParams.getFacetSpec(name);
      if (fspec != null) {
        Filter finalFilter = null;
        if (fspec.isExpandSelection() && filter != null)
          finalFilter = filter;

        FacetCountCollector countCollector = facetHandler.getFacetCountCollectorSource(sel, fspec).getFacetCountCollector((FacetAtomicReader) _context.reader());

        FacetContext facetContext = new FacetContext(countCollector, facetHandler,
            finalFilter.getDocIdSet(_context, null).iterator());

        _facetContexts.add(facetContext);
      }
    }

    _validator = createFacetValidator(_facetContexts);
  }

  @Override
  public boolean acceptsDocsOutOfOrder() {
    return false;
  }

  public Map<String, FacetAccessible> getFacets() {
    updateFacetCollectors();

    Map<String, FacetAccessible> facetMap = new HashMap<String, FacetAccessible>();
    for (Map.Entry<String, Map<AtomicReader, FacetAccessible>> entry : _facetCollectors.entrySet()) {
      FacetHandler<?> handler = _request.getAllFacetHandlerMap().get(entry.getKey());
      FacetSpec spec = _requestParams.getFacetSpec(entry.getKey());
      if (handler != null && spec != null) {
        FacetAccessible merged = new CombinedFacetAccessible(spec, new ArrayList<FacetAccessible>(entry.getValue().values()));
        facetMap.put(entry.getKey(), merged);
      }
    }

    return facetMap;
  }

  private void updateFacetCollectors() {
    for (FacetContext facetContext : _facetContexts) {
      String field = facetContext.getFacetHandler().getName();
      Map<AtomicReader, FacetAccessible> facetColectors = _facetCollectors.get(field);
      if (facetColectors == null) {
        facetColectors = new HashMap<AtomicReader, FacetAccessible>();
        _facetCollectors.put(field, facetColectors);
      }
      if (null != facetContext.getCountCollector()) {
        facetColectors.put(_context.reader(), facetContext.getCountCollector());
      }
    }
  }

  private FacetValidator createFacetValidator(List<FacetContext> facetContexts) throws IOException {
    FacetContext[] sortedContexts = new FacetContext[facetContexts.size()];
    int numPostFilters;
    int i = 0;
    int j = facetContexts.size();

    for (FacetContext facetContext : facetContexts) {
      if (facetContext.getFacetHitIterator() != null) {
        sortedContexts[i] = facetContext;
        i++;
      } else {
        j--;
        sortedContexts[j] = facetContext;
      }
    }
    numPostFilters = i;

    // sort contexts
    Comparator<FacetContext> comparator = new Comparator<FacetContext>() {
      public int compare(FacetContext fhc1, FacetContext fhc2) {
        double selectivity1 = fhc1.getFacetHitIterator().cost();
        double selectivity2 = fhc2.getFacetHitIterator().cost();
        if (selectivity1 < selectivity2) {
          return -1;
        } else if (selectivity1 > selectivity2) {
          return 1;
        }
        return 0;
      }
    };
    Arrays.sort(sortedContexts, 0, numPostFilters, comparator);

    if (numPostFilters == 0) {
      return new NoNeedFacetValidator(sortedContexts);
    } else if (numPostFilters == 1) {
      return new OnePostFilterFacetValidator(sortedContexts);
    } else {
      return new DefaultFacetValidator(sortedContexts, numPostFilters);
    }
  }
}
