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

import it.unimi.dsi.fastutil.ints.IntSet;

import org.apache.log4j.Logger;

import com.google.protobuf.Message;
import com.linkedin.norbert.NorbertException;
import com.linkedin.norbert.javacompat.cluster.ClusterListener;
import com.linkedin.norbert.javacompat.network.PartitionedNetworkClient;
import com.senseidb.search.req.AbstractSenseiRequest;
import com.senseidb.search.req.AbstractSenseiResult;
import com.senseidb.svc.api.SenseiException;

/**
 * @author "Xiaoyang Gu<xgu@linkedin.com>"
 * 
 * @param <REQUEST>
 * @param <RESULT>
 */
public abstract class AbstractSenseiBroker<REQUEST extends AbstractSenseiRequest, RESULT extends AbstractSenseiResult>
    implements ClusterListener, Broker<REQUEST, RESULT>
{
  private final static Logger logger = Logger.getLogger(AbstractSenseiBroker.class);
  protected final PartitionedNetworkClient<String> _networkClient;
  protected volatile IntSet _partitions = null;
  

  /**
   * @param networkClient
   * @param clusterClient
   * @param routerFactory
   * @param scatterGatherHandler
   * @throws NorbertException
   */
  public AbstractSenseiBroker(PartitionedNetworkClient<String> networkClient)
      throws NorbertException
  {
    _networkClient = networkClient;
  }

  /**
   * @return an empty result instance. Used when the request cannot be properly
   *         processed or when the true result is empty.
   */
  public abstract RESULT getEmptyResultInstance();

  /**
   * The method that provides the search service.
   * 
   * @param req
   * @return
   * @throws SenseiException
   */
  public RESULT browse(final REQUEST req) throws SenseiException
  {
    if (_partitions == null)
      throw new SenseiException("Browse called before cluster is connected!");
    try
    {
    	return doBrowse(_networkClient, req, _partitions);
      
    } catch (Exception e)
    {
      throw new SenseiException(e.getMessage(), e);
    }
  }

  protected abstract RESULT doBrowse(PartitionedNetworkClient<String> networkClient, REQUEST req, IntSet partitions) throws Exception;

  public void shutdown()
  {
    logger.info("shutting down broker...");
  }
}
