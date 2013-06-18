/**
 * This software is licensed to you under the Apache License, Version 2.0 (the
 * "Apache License").
 *
 * LinkedIn's contributions are made under the Apache License. If you contribute
 * to the Software, the contributions will be deemed to have been made under the
 * Apache License, unless you expressly indicate otherwise. Please do not make any
 * contributions that would be inconsistent with the Apache License.
 *
 * You may obtain a copy of the Apache License at http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, this software
 * distributed under the Apache License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the Apache
 * License for the specific language governing permissions and limitations for the
 * software governed under the Apache License.
 *
 * © 2012 LinkedIn Corp. All Rights Reserved.  
 */
package com.senseidb.indexing;

import com.codahale.metrics.Counter;
import com.senseidb.metrics.MetricFactory;
import com.senseidb.metrics.MetricName;
import java.io.IOException;
import java.util.Map;

import com.browseengine.bobo.api.BoboIndexReader;
import com.browseengine.bobo.api.BrowseSelection;
import com.browseengine.bobo.facets.data.FacetDataCache;
import com.browseengine.bobo.facets.data.TermLongList;
import com.browseengine.bobo.facets.filter.FacetRangeFilter;
import com.senseidb.plugin.SenseiPlugin;
import com.senseidb.plugin.SenseiPluginRegistry;
import com.senseidb.search.req.SenseiRequest;

public class TimeBasedIndexSelector implements SenseiIndexPruner, SenseiPlugin {

  private static final String TIME_FACET_NAME = "facetName";
  private String facetName;

  private Counter processedReadersCount;
  private Counter filteredReadersCount;

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
    // register jmx monitoring for timers
    MetricName processedReadersMetric = new MetricName("timeBasedIndexPruner","processedReaderCount");
    processedReadersCount = MetricFactory.newCounter(processedReadersMetric);
    MetricName filteredReadersMetric = new MetricName("timeBasedIndexPruner","filteredReaderCount");
    filteredReadersCount = MetricFactory.newCounter(filteredReadersMetric);
  }

  @Override
  public void stop() {
  }

}
