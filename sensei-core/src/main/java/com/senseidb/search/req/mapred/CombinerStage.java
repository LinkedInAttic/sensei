package com.senseidb.search.req.mapred;

/**
 * Tells on which level the combine callback method is called. 
 * @author vzhabiuk
 *
 */
public enum CombinerStage {
  /**
   * After partition was processed
   */
  partitionLevel, 
  /**
   * On the node level
   */
  nodeLevel
}
