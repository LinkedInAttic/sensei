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
package com.senseidb.indexing.activity;

import com.senseidb.indexing.activity.primitives.ActivityPrimitivesStorage;



public class ActivityInMemoryFactory extends ActivityPersistenceFactory {

  protected ActivityInMemoryFactory() {

    super("", new ActivityConfig());

  }
  @Override
  public AggregatesMetadata createAggregatesMetadata(String fieldName) {
    return new InMemoryAggregatesMetadata();
  }
  @Override
  public Metadata getMetadata() {
    return null;
  }
  @Override
  public ActivityPrimitivesStorage getActivivityPrimitivesStorage(String fieldName) {
    return null;
  }
  @Override
  protected CompositeActivityStorage getCompositeStorage() {
    return null;
  }
  private static class InMemoryAggregatesMetadata extends AggregatesMetadata {
    private volatile int currentTime = 0;

    public int getLastUpdatedTime() {
      return currentTime;
    }
    public void updateTime(int currentTime) {
      this.currentTime = currentTime;
    }
  }
}
