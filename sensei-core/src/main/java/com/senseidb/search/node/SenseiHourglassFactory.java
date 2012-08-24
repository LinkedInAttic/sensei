package com.senseidb.search.node;

import java.io.File;
import java.util.List;

import org.apache.log4j.Logger;

import proj.zoie.api.DirectoryManager.DIRECTORY_MODE;
import proj.zoie.api.indexing.ZoieIndexableInterpreter;
import proj.zoie.hourglass.impl.HourGlassScheduler;
import proj.zoie.hourglass.impl.HourGlassScheduler.FREQUENCY;
import proj.zoie.hourglass.impl.Hourglass;
import proj.zoie.hourglass.impl.HourglassDirectoryManagerFactory;
import proj.zoie.hourglass.impl.HourglassListener;
import proj.zoie.impl.indexing.ZoieConfig;

import com.browseengine.bobo.api.BoboIndexReader;

public class SenseiHourglassFactory<T> extends SenseiZoieFactory<T>
{
  private static Logger log = Logger.getLogger(SenseiHourglassFactory.class);

  // format "ss mm hh" meaning at hh:mm:ss time of the day that we roll forward for DAILY rolling
  // if it is hourly rolling, it means at mm:ss time of the hour that we roll forward
  // if it is MINUTELY, it means at ss seond of the minute that we roll forward.
  private final String schedule;
  private final boolean appendOnly;
  private final int trimThreshold;
  private final HourGlassScheduler.FREQUENCY frequency;

  private final List<HourglassListener> hourglassListeners;
  
  
  /**
   * @param idxDir the root directory for Hourglass
   * @param interpreter
   * @param indexReaderDecorator
   * @param zoieConfig
   * @param schedule format: "ss mm hh" meaning at hh:mm:ss time of the day that we roll forward for DAILY rolling;
   * if it is hourly rolling, it means at mm:ss time of the hour that we roll forward;
   * if it is MINUTELY, it means at ss seond of the minute that we roll forward.
   * @param trimThreshold the number of units of rolling periods to keep (for DAILY rolling, we keep trimThreshold number of days of data)
   * @param frequency rolling frequency
   * @param awareSegmentDisposal 
   * @param activityManager 
   */
  @SuppressWarnings("rawtypes")
  public SenseiHourglassFactory(File idxDir, DIRECTORY_MODE dirMode,ZoieIndexableInterpreter<T> interpreter, SenseiIndexReaderDecorator indexReaderDecorator,
                                 ZoieConfig zoieConfig,
                                 String schedule,
                                 boolean appendOnly,
                                 int trimThreshold,
                                 FREQUENCY frequency, List<HourglassListener> hourglassListeners)
  {
    super(idxDir,dirMode,interpreter,indexReaderDecorator,zoieConfig);
    this.schedule = schedule;
    this.appendOnly = appendOnly;
    this.trimThreshold = trimThreshold;
    this.frequency = frequency;
    this.hourglassListeners = hourglassListeners;
    log.info("creating " + this.getClass().getName() + " with schedule: " + schedule
        + " frequency: " + frequency
        + " trimThreshold: " + trimThreshold);
  }
  @Override
  public Hourglass<BoboIndexReader,T> getZoieInstance(int nodeId,int partitionId)
  {
    File partDir = getPath(nodeId,partitionId);
    if(!partDir.exists())
    {
      partDir.mkdirs();
      log.info("nodeId="+nodeId+", partition=" + partitionId + " does not exist, directory created.");
    }
    // format "ss mm hh" meaning at hh:mm:ss time of the day, we roll forward for DAILY rolling
    // if it is hourly rolling, it means at mm:ss time of the hour, we roll forward
    // if it is MINUTELY, it means at ss seond of the minute, we roll forward.
    HourGlassScheduler scheduler = new HourGlassScheduler(frequency, schedule, appendOnly, trimThreshold);
    HourglassDirectoryManagerFactory dirmgr = new HourglassDirectoryManagerFactory(partDir, scheduler,_dirMode);
    log.info("creating Hourglass for nodeId: " + nodeId + " partition: " + partitionId);
    return new Hourglass<BoboIndexReader,T>(dirmgr, _interpreter, _indexReaderDecorator, _zoieConfig, hourglassListeners);
  }
  
  // TODO: change to getDirectoryManager
  public File getPath(int nodeId,int partitionId)
  {
    return getPath(_idxDir,nodeId,partitionId);
  }
}
