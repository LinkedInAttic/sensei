package com.senseidb.util;

import java.lang.Thread.UncaughtExceptionHandler;

import org.apache.log4j.Logger;
import com.senseidb.util.SenseiUncaughtExceptionHandler;
/**
 * The default UncaughtExceptionHandler  for Sensei process {@link UncaughtExceptionHandler}
 * @author vzhabiuk
 *
 */
public class SenseiUncaughtExceptionHandler implements UncaughtExceptionHandler{
  private static Logger logger = Logger.getLogger(SenseiUncaughtExceptionHandler.class);
  private static SenseiUncaughtExceptionHandler instance = new SenseiUncaughtExceptionHandler();
  
  public static SenseiUncaughtExceptionHandler getInstance() {
    return instance;
  }
  public static void setAsDefaultForAllThreads() {
    synchronized (SenseiUncaughtExceptionHandler.class) {
      if (Thread.getDefaultUncaughtExceptionHandler() != instance) {
        Thread.setDefaultUncaughtExceptionHandler(instance);
      }
    }
  }
  @Override
  public void uncaughtException(Thread thread, Throwable throwable) {
    logger.fatal(String.format("An uncaught throwable was thrown for the thread - %s", thread.toString()), throwable);    
  }
}
