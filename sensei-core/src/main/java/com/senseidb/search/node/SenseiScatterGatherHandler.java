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
package com.senseidb.search.node;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.zip.GZIPInputStream;

import com.browseengine.bobo.api.FacetSpec;
import com.linkedin.norbert.javacompat.cluster.Node;
import org.apache.log4j.Logger;
import org.apache.lucene.document.Document;

import com.senseidb.conf.SenseiSchema;
import com.senseidb.search.req.ErrorType;
import com.senseidb.search.req.SenseiError;
import com.senseidb.search.req.SenseiHit;
import com.senseidb.search.req.SenseiRequest;
import com.senseidb.search.req.SenseiResult;

public class SenseiScatterGatherHandler extends AbstractSenseiScatterGatherHandler<SenseiRequest, SenseiResult>
{

  private final static Logger logger = Logger.getLogger(SenseiScatterGatherHandler.class);

  private final SenseiRequestScatterRewriter _reqRewriter;
  
  public SenseiScatterGatherHandler(SenseiRequest request, SenseiRequestScatterRewriter reqRewriter, long timeoutMillis)
  {
      super(request, timeoutMillis);
      _reqRewriter = reqRewriter;
  }
  
  @Override
  public SenseiResult mergeResults(SenseiRequest request, List<SenseiResult> resultList)
  {
    SenseiResult res = ResultMerger.merge(request, resultList, false);

    if (request.isFetchStoredFields()) {  // Decompress binary data.
      for(SenseiHit hit : res.getSenseiHits()) {
        try
        {
          Document doc = hit.getStoredFields();
          byte[] dataBytes = doc.getBinaryValue(SenseiSchema.SRC_DATA_COMPRESSED_FIELD_NAME);
          if (dataBytes!=null && dataBytes.length>0){
            ByteArrayOutputStream bout = new ByteArrayOutputStream();
            byte[] buf = new byte[1024];  // 1k buffer
            ByteArrayInputStream bin = new ByteArrayInputStream(dataBytes);
            GZIPInputStream gzipStream = new GZIPInputStream(bin);

            int len;
            while ((len = gzipStream.read(buf)) > 0) {
              bout.write(buf, 0, len);
            }
            bout.flush();

            byte[] uncompressed = bout.toByteArray();
            hit.setSrcData(new String(uncompressed,"UTF-8"));
          }
          else {
            dataBytes = doc.getBinaryValue(SenseiSchema.SRC_DATA_FIELD_NAME);
            if (dataBytes!=null && dataBytes.length>0) {
              hit.setSrcData(new String(dataBytes,"UTF-8"));
            }
          }
          doc.removeFields(SenseiSchema.SRC_DATA_COMPRESSED_FIELD_NAME);
          doc.removeFields(SenseiSchema.SRC_DATA_FIELD_NAME);
        }
        catch(Exception e)
        {
          res.addError(new SenseiError(e.getMessage(),ErrorType.BrokerGatherError));
          logger.error(e.getMessage(),e);
        }
      }
    }

    return res;
  }

  @Override
  public SenseiRequest customizeRequest(SenseiRequest senseiReq, Node node, Set<Integer> partitions)
  {
    // Rewrite facet max count.
    Map<String, FacetSpec> facetSpecs = senseiReq.getFacetSpecs();
    if (facetSpecs != null) {
      for (Map.Entry<String, FacetSpec> entry : facetSpecs.entrySet()) {
        FacetSpec spec = entry.getValue();
        if (spec != null)
          spec.setMaxCount(0);
      }
    }

    int oldOffset = senseiReq.getOffset();
    int oldCount = senseiReq.getCount();
    if (_reqRewriter != null)
    {
      senseiReq = _reqRewriter.rewrite(senseiReq, node, partitions);
    }

    // customize only if user wants hits
    if (oldCount > 0)
    {
      senseiReq.setOffset(0);
      senseiReq.setCount(oldOffset + oldCount);
    }
    senseiReq.setPartitions(partitions);

    if (logger.isDebugEnabled())
    {
      logger.debug("scattering to partitions: " + partitions.toString());
    }
    return senseiReq;
  }

}
