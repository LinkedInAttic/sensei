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

package com.senseidb.indexing.activity.time;

import com.senseidb.indexing.activity.facet.SynchronizedActivityRangeFacetHandler;
import com.senseidb.indexing.activity.primitives.ActivityIntValues;
import com.senseidb.indexing.activity.time.TimeAggregatedActivityValues;
import com.senseidb.indexing.activity.time.TimeAggregatedActivityValues.IntValueHolder;

public class ActivityIntValuesSynchronizedDecorator extends ActivityIntValues {
  private final ActivityIntValues decorated;

  @Override
  public void init(int capacity) {
    decorated.init(capacity);
  }

  public void init() {
    decorated.init();
  }
  
  @Override
  public boolean update(int index, Object value) {
    synchronized(SynchronizedActivityRangeFacetHandler.GLOBAL_ACTIVITY_TEST_LOCK) {
      //System.out.println("!!!Update" + value);
      return decorated.update(index, value);
    }
  }

 
  public void delete(int index) {
    synchronized(SynchronizedActivityRangeFacetHandler.GLOBAL_ACTIVITY_TEST_LOCK) {
      decorated.delete(index);
    }
  }
  protected ActivityIntValuesSynchronizedDecorator(ActivityIntValues decorated) {
    this.decorated = decorated;
  }
  

 
  public Runnable prepareFlush() {
    return this.decorated.prepareFlush();
  }

  public int getIntValue(int index) {
    synchronized(SynchronizedActivityRangeFacetHandler.GLOBAL_ACTIVITY_TEST_LOCK) {
      return this.decorated.getIntValue(index);
    }
  }

 


  public int[] getFieldValues() {
   return decorated.getFieldValues();
  }

  public void setFieldValues(int[] fieldValues) {
    decorated.setFieldValues(fieldValues);
  }

 
  @Override
  public void close() {
    decorated.close();
  }

  @Override
  public String getFieldName() {
    return decorated.getFieldName();
  }
  public static void decorate( TimeAggregatedActivityValues timeAggregatedActivityValues) {
    timeAggregatedActivityValues.defaultIntValues = new ActivityIntValuesSynchronizedDecorator(timeAggregatedActivityValues.defaultIntValues);
    for (IntValueHolder intValueHolder :  timeAggregatedActivityValues.intActivityValues) {
      intValueHolder.activityIntValues = new ActivityIntValuesSynchronizedDecorator(intValueHolder.activityIntValues);
    }
  }
}
