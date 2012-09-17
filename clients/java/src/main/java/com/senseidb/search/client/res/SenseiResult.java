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

package com.senseidb.search.client.res;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.json.JSONObject;

import com.senseidb.search.client.json.JsonField;

public class SenseiResult {
    private Long tid;
    private Integer totaldocs;

    private Integer numhits;
    private Integer numgroups;
    private List<SenseiHit> hits = new ArrayList<SenseiHit>();
    @JsonField("parsedquery")
    private String parsedQuery;
    private Long time;
    private Map<String, List<FacetResult>> facets;

    private JSONObject mapReduceResult;
    private Integer errorCode;
    private List<Error> errors;

    @Override
    public String toString() {
        return "SenseiResult [tid=" + tid + ", totaldocs=" + totaldocs + ", numhits=" + numhits + ", numgroups="
                + numgroups + ", \nhits=" + hits + "\nmapReduceResult=" + mapReduceResult + ",\n parsedQuery=" + parsedQuery + ", time=" + time + ", \nfacets="
                + facets + "]";
    }
    public Long getTid() {
        return tid;
    }
    public void setTid(Long tid) {
        this.tid = tid;
    }
    public Integer getTotaldocs() {
        return totaldocs;
    }
    public void setTotaldocs(Integer totaldocs) {
        this.totaldocs = totaldocs;
    }
    public Integer getNumhits() {
        return numhits;
    }
    public void setNumhits(Integer numhits) {
        this.numhits = numhits;
    }
    public Integer getNumgroups() {
        return numgroups;
    }
    public void setNumgroups(Integer numgroups) {
        this.numgroups = numgroups;
    }
    public List<SenseiHit> getHits() {
        return hits;
    }
    public void setHits(List<SenseiHit> hits) {
        this.hits = hits;
    }
    public String getParsedQuery() {
        return parsedQuery;
    }
    public void setParsedQuery(String parsedQuery) {
        this.parsedQuery = parsedQuery;
    }
    public Long getTime() {
        return time;
    }
    public void setTime(Long time) {
        this.time = time;
    }
    public Map<String, List<FacetResult>> getFacets() {
        return facets;
    }
    public void setFacets(Map<String, List<FacetResult>> facets) {
        this.facets = facets;
    }

    public JSONObject getMapReduceResult() {
      return mapReduceResult;
    }
    public void setMapReduceResult(JSONObject mapReduceResult) {
      this.mapReduceResult = mapReduceResult;
    }
    public Integer getErrorCode() {
      return errorCode;
    }
    public void setErrorCode(Integer errorCode) {
      this.errorCode = errorCode;
    }
    public List<Error> getErrors() {
      if (errors == null)
        errors = new ArrayList<Error>();

      return errors;
    }
    public void setErrors(List<Error> errors) {
      this.errors = errors;
    }   

}
