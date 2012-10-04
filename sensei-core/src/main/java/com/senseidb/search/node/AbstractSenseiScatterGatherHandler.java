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

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import com.linkedin.norbert.javacompat.cluster.Node;
import com.linkedin.norbert.network.ResponseIterator;
import com.linkedin.norbert.javacompat.network.ScatterGatherHandler;
import com.senseidb.search.req.AbstractSenseiRequest;
import com.senseidb.search.req.AbstractSenseiResult;
import com.senseidb.search.req.ErrorType;
import com.senseidb.search.req.SenseiError;

public abstract class AbstractSenseiScatterGatherHandler<REQUEST extends AbstractSenseiRequest, RESULT extends AbstractSenseiResult>
    implements ScatterGatherHandler<REQUEST, RESULT, RESULT, Integer>
{

  private final static Logger logger = Logger.getLogger(AbstractSenseiScatterGatherHandler.class);

  private final static long TIMEOUT_MILLIS = 8000L;

  private final REQUEST _request;

  private long _timeoutMillis = TIMEOUT_MILLIS;

  public AbstractSenseiScatterGatherHandler(REQUEST request) {
    _request = request;
  }

  public void setTimeoutMillis(long timeoutMillis)
  {
    _timeoutMillis = timeoutMillis;
  }

  public long getTimeoutMillis()
  {
    return _timeoutMillis;
  }

  /**
   * Merge results on the client/broker side. It likely works differently from
   * the one in the search node.
   * 
   * @param resultList
   *          the list of results from all the requested partitions.
   * @return one single result instance that is merged from the result list.
   */
  public abstract RESULT mergeResults(REQUEST request, List<RESULT> resultList);


  public abstract REQUEST customizeRequest(REQUEST request, Node node, Set<Integer> partitions);

  /*
   * (non-Javadoc)
   * 
   * @see
   * com.linkedin.norbert.network.javaapi.ScatterGatherHandler#gatherResponses
   * (com.google.protobuf.Message,
   * com.linkedin.norbert.network.ResponseIterator)
   */

    @Override
    public RESULT gatherResponses(ResponseIterator<RESULT> iter) throws Exception {
        boolean debugmode = logger.isDebugEnabled();
        int timeOuts = 0;;
        List<RESULT> boboBrowseList = new ArrayList<RESULT>();
        while (iter.hasNext())
        {
          RESULT result = iter.next(_timeoutMillis > 0 ? _timeoutMillis : Long.MAX_VALUE, TimeUnit.MILLISECONDS);
          if (result == null)
          {
            timeOuts++;
            logger.error("Request Timed Out");
          } else
          {
            boboBrowseList.add(result);
          }
        }
        RESULT res = mergeResults(_request, boboBrowseList);
        res.addError(new SenseiError("Request timeout", ErrorType.BrokerTimeout));
        if (debugmode)
        {
          logger.debug("merged results: " + res);
          logger.debug("Merging the sensei Results for the input senseiRequest");
        }
        return res;
    }
}
