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
