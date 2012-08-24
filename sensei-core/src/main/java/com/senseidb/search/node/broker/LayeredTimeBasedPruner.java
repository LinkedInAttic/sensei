package com.senseidb.search.node.broker;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.browseengine.bobo.api.BrowseSelection;
import com.browseengine.bobo.facets.filter.FacetRangeFilter;
import com.senseidb.indexing.activity.time.Clock;
import com.senseidb.plugin.SenseiPlugin;
import com.senseidb.plugin.SenseiPluginRegistry;
import com.senseidb.search.req.SenseiRequest;

public class LayeredTimeBasedPruner implements LayeredClusterPruner, SenseiPlugin {
  private static final String CLUSTERS = "clusters";
  private List<String> clusters = new ArrayList<String>();
  private Map<String, DateRange> clusterRanges = new HashMap<String, DateRange> ();
  private static final String TIME_COLUMN = "timeColumn";
  private String timeColumn;
  private static class DateRange {
    private long startTime;
    private long endTime;
    public static DateRange valueOf(String dateStr) {
      String startTimeStr = dateStr.split("-")[0];
      String endTimeStr = dateStr.split("-")[1];
      DateRange ret = new DateRange();
      ret.startTime = Long.parseLong(startTimeStr) * 24 * 60 * 60 * 1000;
      ret.endTime = Long.parseLong(endTimeStr) * 24 * 60 * 60 * 1000;
      return ret;
    }  
  }
  @Override
  public void init(Map<String, String> config, SenseiPluginRegistry pluginRegistry) {
    String clustersConfig = config.get(CLUSTERS);
    timeColumn = config.get(TIME_COLUMN);
    if (timeColumn == null) {
      throw new IllegalArgumentException(TIME_COLUMN + " param should be present");
    }
    if (clustersConfig == null) {
      throw new IllegalArgumentException(CLUSTERS + " param should be present");
    }
    for (String cluster : clustersConfig.split(",")) {
      String trimmed = cluster.trim();
      if (trimmed.length() > 0) {
        clusters.add(trimmed);
        String dayRange = config.get("daysRange." + trimmed);
        if (dayRange == null || dayRange.contains("-")) {
          throw new IllegalStateException("The dayRange should be specified for the cluster - " + trimmed + ". And it should have a format \"0-20\" where 0 and 20 are number of days");
        }
        clusterRanges.put(trimmed, DateRange.valueOf(dayRange));
      }
    }
  }

  @Override
  public void start() {    
  }

  @Override
  public void stop() {    
  }

  @Override
  public List<String> pruneClusters(SenseiRequest request, List<String> clusters) {
    BrowseSelection selection = request.getSelection(timeColumn);
    if (selection == null) {
      return clusters;
    }
    String[] rangeStrings = FacetRangeFilter.getRangeStrings(selection.getValues()[0]);
    final long start = getStartTime(rangeStrings);
    final long end = getEndTime(rangeStrings);
    List<String> ret = new ArrayList<String>();
    for (String cluster : clusters) {
      DateRange clusterRange = clusterRanges.get(cluster);
      if (Clock.getTime() + clusterRange.startTime > end || Clock.getTime() + clusterRange.endTime < start) {
        //skipping cluster
      } else {
        ret.add(cluster);
      }
    }
    return ret;
  }

  @Override
  public boolean clusterPrioritiesEqual(SenseiRequest request) {    
    return false;
  }
  public static long getStartTime(String[] rangeStrings) {
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
  public static long getEndTime(String[] rangeStrings) {
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
}
