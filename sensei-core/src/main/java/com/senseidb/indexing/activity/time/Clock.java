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

import java.text.ParseException;
import java.text.SimpleDateFormat;

/**
 * This class should be used instead of System.currentTimeInMillis. It also keep track of minute count since Jan 1st 2010
 * @author vzhabiuk
 *
 */
public class Clock {
  private static volatile Long predefinedTime;
  private static volatile Integer predefinedTimeInMinutes;
  private static volatile long startTime;
  static {
    SimpleDateFormat formatter = new SimpleDateFormat("yyyy.MM.dd.HH.mm.ss");
    try {
      startTime = formatter.parse("2010.01.01.00.00.00").getTime();     
    } catch (ParseException e) {
      throw new RuntimeException(e);
    }
  }
  public static int getCurrentTimeInMinutes() {
    if (predefinedTimeInMinutes != null) {
      return predefinedTimeInMinutes;
    }
    return (int)((getTime() - startTime) / 1000/60);
  }
  public static Integer getPredefinedTimeInMinutes() {
    return predefinedTimeInMinutes;
  }
  public static void setPredefinedTimeInMinutes(Integer predefinedTimeInMinutes) {
    Clock.predefinedTimeInMinutes = predefinedTimeInMinutes;
  }
  public static long getTime() {
    if (predefinedTime == null) {
      return System.currentTimeMillis();
    }
    return predefinedTime;
  }
  
  public static void setPredefinedTime(Long predefinedTime) {
    Clock.predefinedTime = predefinedTime;
  }  
}
