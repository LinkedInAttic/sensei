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
package com.senseidb.svc.impl;

import com.senseidb.metrics.MetricFactory;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import com.linkedin.norbert.javacompat.network.RequestHandler;
import com.senseidb.metrics.MetricsConstants;
import com.senseidb.search.req.AbstractSenseiRequest;
import com.senseidb.search.req.AbstractSenseiResult;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.Timer;

public final class SenseiCoreServiceMessageHandler<REQUEST extends AbstractSenseiRequest, RESULT extends AbstractSenseiResult> implements RequestHandler<REQUEST, RESULT> {
	private static final Logger logger = Logger.getLogger(SenseiCoreServiceMessageHandler.class);
    private final AbstractSenseiCoreService<REQUEST, RESULT> _svc;

    private final Timer _totalSearchTimer;

    public SenseiCoreServiceMessageHandler(AbstractSenseiCoreService<REQUEST, RESULT> svc){
		  _svc = svc;
      MetricName metricName = new MetricName(MetricsConstants.Domain,"timer","total-search-time","node");
      _totalSearchTimer = MetricFactory.newTimer(metricName, TimeUnit.MILLISECONDS, TimeUnit.SECONDS);
	  }

    @Override
    public RESULT handleRequest(final REQUEST request) throws Exception {
        return _totalSearchTimer.time(new Callable<RESULT>(){

            @Override
            public RESULT call() throws Exception {
                return _svc.execute(request);
            }
        });
    }
}
