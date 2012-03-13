package com.senseidb.indexing.activity.time;

import java.text.ParseException;
import java.text.SimpleDateFormat;

public class Clock {
  private static Long predefinedTime;
  private static long startTime;
  static {
    SimpleDateFormat formatter = new SimpleDateFormat("yyyy.MM.dd.HH.mm.ss");
    try {
      startTime = formatter.parse("2010.01.01.00.00.00").getTime();     
    } catch (ParseException e) {
      throw new RuntimeException(e);
    }
  }
  public static int getCurrentTimeInMinutes() {
    return (int)((getTime() - startTime) / 1000/60);
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
