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

import java.io.File;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.util.Version;
import org.json.JSONObject;

import proj.zoie.api.indexing.AbstractZoieIndexableInterpreter;
import proj.zoie.api.indexing.ZoieIndexable;
import proj.zoie.impl.indexing.SimpleReaderCache;
import proj.zoie.impl.indexing.ZoieConfig;

import com.senseidb.search.node.SenseiCore;
import com.senseidb.search.node.impl.DefaultJsonQueryBuilderFactory;
import com.senseidb.search.node.impl.DemoZoieSystemFactory;
import com.senseidb.search.req.SenseiRequest;
import com.senseidb.search.req.SenseiResult;
import com.senseidb.search.req.SenseiSystemInfo;
import com.senseidb.svc.api.SenseiException;
import com.senseidb.svc.api.SenseiService;

public class LocalQueryOnlySenseiServiceImpl implements SenseiService {

  private CoreSenseiServiceImpl _coreService;
  private final SenseiCore _core;
  public LocalQueryOnlySenseiServiceImpl(File idxDir) throws Exception{
    ZoieConfig zoieConfig = new ZoieConfig();
    zoieConfig.setReadercachefactory(SimpleReaderCache.FACTORY);
    DemoZoieSystemFactory zoieFactory = new DemoZoieSystemFactory(idxDir,new AbstractZoieIndexableInterpreter<JSONObject>(){
      @Override
      public ZoieIndexable convertAndInterpret(JSONObject src) {
        return null;
      }
    },zoieConfig);
    QueryParser queryParser = new QueryParser(Version.LUCENE_35,"contents", new StandardAnalyzer(Version.LUCENE_35));
    DefaultJsonQueryBuilderFactory queryBuilderFactory = new DefaultJsonQueryBuilderFactory(queryParser);
    _core = new SenseiCore(1,new int[]{0},zoieFactory,null,queryBuilderFactory);
    _coreService = new CoreSenseiServiceImpl(_core);
    _core.start();
  }
  
  @Override
  public SenseiResult doQuery(SenseiRequest req) throws SenseiException {
    return _coreService.execute(req);
  }

  @Override
  public SenseiSystemInfo getSystemInfo() throws SenseiException {
    return null;
  }

  @Override
  public void shutdown() {
    _core.shutdown();
  }

  public static void main(String[] args) throws Exception{
    File idxDir = new File("/tmp/sensei-example-cars/node1/shard0");
    SenseiService svc = new LocalQueryOnlySenseiServiceImpl(idxDir);
    SenseiResult res = svc.doQuery(new SenseiRequest());
    System.out.println(res.getTotalDocs());
    svc.shutdown();
  }
}
