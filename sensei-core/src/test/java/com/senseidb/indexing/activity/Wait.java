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

import junit.framework.Assert;

public class Wait {
  public static void until(long timeToWait, String failureMessage, Condition condition) {
    long time = System.currentTimeMillis();
    while (System.currentTimeMillis() - time <= timeToWait) {
      if (condition.evaluate()) {
        return;
      }
      try {
        Thread.sleep(10L);
      } catch (InterruptedException e) {
        throw new IllegalStateException(e);
      }
    }
    Assert.assertTrue(failureMessage, condition.evaluate());
  }
  
  public static interface Condition {
    public boolean evaluate();
  }
}
