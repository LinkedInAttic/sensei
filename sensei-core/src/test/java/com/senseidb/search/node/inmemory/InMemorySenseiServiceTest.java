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

package com.senseidb.search.node.inmemory;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import org.json.JSONObject;

import com.senseidb.search.req.SenseiResult;

import junit.framework.TestCase;

public class InMemorySenseiServiceTest extends TestCase {
  private InMemorySenseiService inMemorySenseiService;
  private List<JSONObject> docs;
  @Override
  protected void setUp() throws Exception {
    inMemorySenseiService =  InMemorySenseiService.valueOf(new File(
        InMemoryIndexPerfTest.class.getClassLoader().getResource("test-conf/node1/").toURI()));
    LineIterator lineIterator = FileUtils.lineIterator(new File(InMemoryIndexPerfTest.class.getClassLoader().getResource("data/test_data.json").toURI()));
    int i = 0;
    docs = new ArrayList<JSONObject>();
    while(lineIterator.hasNext() && i < 100) {
      String car = lineIterator.next();
      if (car != null && car.contains("{"))
      docs.add(new JSONObject(car));
      i++;
    }
    lineIterator.close();
  }
  public void test1() {
    SenseiResult result = inMemorySenseiService.doQuery(InMemoryIndexPerfTest.getRequest(), docs); 
    assertEquals(16, result.getNumHits());
    assertEquals(100, result.getTotalDocs());
  }
}
