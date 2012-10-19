package com.senseidb.indexing;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import com.browseengine.bobo.api.BoboIndexReader;
import com.browseengine.bobo.api.BrowseSelection;
import com.browseengine.bobo.facets.data.FacetDataCache;
import com.browseengine.bobo.facets.data.TermLongList;
import com.browseengine.bobo.facets.filter.FacetRangeFilter;
import com.senseidb.metrics.MetricsConstants;
import com.senseidb.plugin.SenseiPlugin;
import com.senseidb.plugin.SenseiPluginRegistry;
import com.senseidb.search.req.SenseiRequest;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.MetricName;

public class TimeBasedIndexSelector implements SenseiIndexPruner, SenseiPlugin {

  private static final String TIME_FACET_NAME = "facetName";
  private String facetName;
  private static Counter processedReadersCount;
  private static Counter filteredReadersCount;
  static{
    // register jmx monitoring for timers
      MetricName processedReadersMetric = new MetricName(MetricsConstants.Domain, "timeBasedIndexPruner","processedReaderCount");
      processedReadersCount = Metrics.newCounter(processedReadersMetric);
      MetricName filteredReadersMetric = new MetricName(MetricsConstants.Domain,"timeBasedIndexPruner","filteredReaderCount");
      filteredReadersCount = Metrics.newCounter(filteredReadersMetric);
  
  }
  private IndexReaderSelector defaultReaderSelector = new IndexReaderSelector() {
    @Override
    public boolean isSelected(BoboIndexReader reader) throws IOException {
      return true;
    }
  };
  @Override
  public IndexReaderSelector getReaderSelector(SenseiRequest req) {
    BrowseSelection selection = req.getSelection(facetName);
    if (selection == null || selection.getValues() == null || selection.getValues().length == 0) {
      return defaultReaderSelector;
    }
     String[] rangeStrings = FacetRangeFilter.getRangeStrings(selection.getValues()[0]);
     final long start = getStartTime(rangeStrings);
     final long end = getEndTime(rangeStrings);
    return new IndexReaderSelector() {      
      @Override
      public boolean isSelected(BoboIndexReader reader) throws IOException {
        processedReadersCount.inc();
        Object facetDataObj = reader.getFacetData(facetName);
        if (facetDataObj == null || !(facetDataObj instanceof FacetDataCache)) {
          throw new IllegalStateException("Couldn't extract the facet data cache for the facet - " + facetName);
        }
        FacetDataCache facetDataCache= (FacetDataCache)reader.getFacetData(facetName);
        if (!(facetDataCache.valArray instanceof TermLongList)) {
          throw new IllegalStateException("Currently only the long field is supported for the time facet - " + facetName);
        }
        long[] elements = ((TermLongList)facetDataCache.valArray).getElements();
        if (elements.length < 2) {
          filteredReadersCount.inc();          
          return false;
        }
        if (elements[1] > end || elements[elements.length - 1] < start) {
          filteredReadersCount.inc();          
          return false;
        }                
        return true;
      }
    };
  }

  @Override
  public void sort(List<BoboIndexReader> readers) {
    // do nothing
  }

  private long getStartTime(String[] rangeStrings) {
    long start;
    if ("*".equals(rangeStrings[0])) {
       start = Long.MIN_VALUE;
     } else {
       start = Long.parseLong(rangeStrings[0]);
       if ("true".equals(rangeStrings[2])) {
         start--;
       }
     }
    return start;
  }
  private long getEndTime(String[] rangeStrings) {
    long end;
    if ("*".equals(rangeStrings[1])) {
      end = Long.MAX_VALUE;
     } else {
       end = Long.parseLong(rangeStrings[1]);
       if ("true".equals(rangeStrings[3])) {
         end++;
       }
     }
    return end;
  }
  @Override
  public void init(Map<String, String> config, SenseiPluginRegistry pluginRegistry) {
     facetName = config.get(TIME_FACET_NAME);
    
  }

  @Override
  public void start() {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void stop() {
    // TODO Auto-generated method stub
    
  }

}
