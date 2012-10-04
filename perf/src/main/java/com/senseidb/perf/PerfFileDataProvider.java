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

package com.senseidb.perf;

import java.io.File;
import java.util.Comparator;
import java.util.concurrent.LinkedBlockingQueue;

import org.json.JSONObject;

import proj.zoie.api.DataConsumer.DataEvent;

import com.senseidb.gateway.file.LinedJsonFileDataProvider;


public class PerfFileDataProvider extends LinedJsonFileDataProvider {

  public static LinkedBlockingQueue<JSONObject> queue;


  public PerfFileDataProvider(Comparator<String> versionComparator, File file, long startingOffset, LinkedBlockingQueue<JSONObject> queue) {
    super(versionComparator, file, startingOffset);
    this.queue = queue; 
  }
  
 
  
  
  @Override
  public DataEvent<JSONObject> next() {
    JSONObject object = null;
    try {
      object = queue.take();
    } catch (InterruptedException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }    
    if (_offset % 10000 == 0) {
      System.out.println("Indexed " + _offset + " documents. Queue size = " + queue.size());
    }
    if (object != null) {
      return new DataEvent<JSONObject>(object, String.valueOf(_offset++));
    }
    
    return super.next();
  }

 
}
