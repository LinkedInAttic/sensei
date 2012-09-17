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
package com.senseidb.servlet;

import java.io.OutputStream;

import javax.servlet.http.HttpServletRequest;

import org.apache.commons.configuration.DataConfiguration;
import org.apache.commons.configuration.web.ServletRequestConfiguration;

import com.senseidb.search.req.SenseiRequest;
import com.senseidb.search.req.SenseiResult;
import com.senseidb.search.req.SenseiSystemInfo;

public abstract class AbstractSenseiRestServlet extends AbstractSenseiClientServlet {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	abstract protected SenseiRequest buildSenseiRequest(DataConfiguration params) throws Exception;
	
	@Override
	protected SenseiRequest buildSenseiRequest(HttpServletRequest req)
			throws Exception {
		DataConfiguration params = new DataConfiguration(new ServletRequestConfiguration(req));
		return buildSenseiRequest(params);
	}
	
	abstract protected String buildResultString(HttpServletRequest httpReq, SenseiRequest req,SenseiResult res) throws Exception;

	abstract protected String buildResultString(HttpServletRequest httpReq, SenseiSystemInfo info) throws Exception;

	@Override
	protected void convertResult(HttpServletRequest httpReq, SenseiSystemInfo info, OutputStream ostream)
			throws Exception {
		String outString = buildResultString(httpReq, info);
		ostream.write(outString.getBytes("UTF-8"));
	}

	@Override
	protected void convertResult(HttpServletRequest httpReq, SenseiRequest req,SenseiResult res, OutputStream ostream)
			throws Exception {
		String outString = buildResultString(httpReq, req,res);
		ostream.write(outString.getBytes("UTF-8"));
	}
}
