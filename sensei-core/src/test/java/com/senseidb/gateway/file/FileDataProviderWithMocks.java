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
package com.senseidb.gateway.file;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.SynchronousQueue;

import org.json.JSONException;
import org.json.JSONObject;

import proj.zoie.api.DataConsumer.DataEvent;


public class FileDataProviderWithMocks extends LinedJsonFileDataProvider {

  private LinkedBlockingQueue<JSONObject> queue;


  public FileDataProviderWithMocks(Comparator<String> versionComparator, File file, long startingOffset) {
    super(versionComparator, file, startingOffset);   
    queue = new LinkedBlockingQueue<JSONObject>(30000);
    mockRequests.add(queue); 
    instances.add(this);
  }
  public static List<FileDataProviderWithMocks> instances = new ArrayList<FileDataProviderWithMocks>();
  private static List<Queue<JSONObject>> mockRequests = new ArrayList<Queue<JSONObject>>();
  public static void resetOffset(long newOffset) {
    for (FileDataProviderWithMocks instance : instances) {
      instance._offset = newOffset;
    }
  }
  
  @Override
  public DataEvent<JSONObject> next() {
    JSONObject object = queue.poll();    
    if (object != null) {
      return new DataEvent<JSONObject>(object, String.valueOf(_offset++));
    }
    return super.next();
  }
  public static void add(JSONObject obj) {
    for (Queue<JSONObject> it : mockRequests) {
      it.add(clone(obj));
    }
  }
  private static JSONObject clone(JSONObject obj) {
     JSONObject ret = new JSONObject();
     for (String key : JSONObject.getNames(obj)) {
       try {
        ret.put(key, obj.opt(key));
      } catch (JSONException ex) {
        throw new RuntimeException(ex);
      }
     }
    return ret;
  }
  public long getOffset() {
    return _offset;
  }
}
