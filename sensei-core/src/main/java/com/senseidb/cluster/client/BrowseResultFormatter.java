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
package com.senseidb.cluster.client;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.browseengine.bobo.api.BrowseFacet;
import com.browseengine.bobo.api.BrowseHit;
import com.browseengine.bobo.api.BrowseResult;
import com.browseengine.bobo.api.FacetAccessible;

public class BrowseResultFormatter{
    
    public static String formatResults(BrowseResult res) {
            StringBuffer sb = new StringBuffer();
            sb.append(res.getNumHits());
            sb.append(" hits out of ");
            sb.append(res.getTotalDocs());
            if (res.getNumGroups() > 0) {
              sb.append(" docs and in ");
              sb.append(res.getNumGroups());
              sb.append(" groups\n");
            }
            else
              sb.append(" docs\n");
            BrowseHit[] hits = res.getHits();
            Map<String,FacetAccessible> map = res.getFacetMap();
            for(Entry<String,FacetAccessible> entry : map.entrySet()) {
            	    String key = entry.getKey();
                    FacetAccessible fa = entry.getValue();
                    sb.append(key).append("\n");
                    List<BrowseFacet> lf = fa.getFacets();
                    for(BrowseFacet bf : lf) {
                       sb.append("\t").append(bf).append("\n");
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
            if (hit.getGroupHitsCount() > 0) {
              sb.append("\t group: ");
              sb.append(hit.getGroupValue());
              sb.append(" hit count: ");
              sb.append(hit.getGroupHitsCount());
              sb.append('\n');
            }
            Map<String, String[]> fields = hit.getFieldValues();
            if (fields!=null){
              
              for(Entry<String,String[]> entry: fields.entrySet()) {
            	    String key = entry.getKey();
                    sb.append("\t").append(key).append(" :");
                    String[] values = entry.getValue();
                    sb.append(Arrays.toString(values));
                    sb.append("\n");
              }
            }
            return sb;
    }
}
