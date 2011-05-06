package com.sensei.search.cluster.client;

import java.util.List;
import java.util.Map;
import java.util.Set;

import com.browseengine.bobo.api.BrowseFacet;
import com.browseengine.bobo.api.BrowseHit;
import com.browseengine.bobo.api.BrowseResult;
import com.browseengine.bobo.api.FacetAccessible;

public class BrowseResultFormatter{
    
    static String formatResults(BrowseResult res) {
            StringBuffer sb = new StringBuffer();
            sb.append(res.getNumHits());
            sb.append(" hits out of ");
            sb.append(res.getTotalDocs());
            sb.append(" docs\n");
            BrowseHit[] hits = res.getHits();
            Map<String,FacetAccessible> map = res.getFacetMap();
            Set<String> keys = map.keySet();
            for(String key : keys) {
                    FacetAccessible fa = map.get(key);
                    sb.append(key + "\n");
                    List<BrowseFacet> lf = fa.getFacets();
                    for(BrowseFacet bf : lf) {
                            sb.append("\t" + bf + "\n");
                    }
            }
            for(BrowseHit hit : hits) {
                    sb.append("------------\n");
                    sb.append(formatHit(hit));
                    sb.append("\n");
            }
            sb.append("*****************************\n");
            return sb.toString();
    }
    
    static StringBuffer formatHit(BrowseHit hit) {
            StringBuffer sb = new StringBuffer();
            Map<String, String[]> fields = hit.getFieldValues();
            if (fields!=null){
              Set<String> keys = fields.keySet();
              for(String key : keys) {
                    sb.append("\t" + key + " :");
                    String[] values = fields.get(key);
                    for(String value : values)
                    {
                            sb.append(" " + value);
                    }
                    sb.append("\n");
              }
            }
            return sb;
    }
}
