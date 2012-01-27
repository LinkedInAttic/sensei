package com.sensei.indexing.hadoop.demo;



import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.senseidb.indexing.hadoop.job.MapReduceJob;
import com.senseidb.indexing.hadoop.util.PropertiesLoader;


public class CarDemo extends MapReduceJob implements Tool {
  

  public int run(String[] args) throws Exception {
    JobConf conf = createJob(CarDemo.class);
    
    conf.setJobName("CarDemo");
    JobClient.runJob(conf);
    return 0;
  }
  

  public static void main(String[] args) throws Exception {
    long start = System.currentTimeMillis();
    Configuration conf = PropertiesLoader.loadProperties("conf/JobCarDemo.job");
    int res = ToolRunner.run(conf, new CarDemo(), args);
    long end = System.currentTimeMillis();
    System.out.println("Total time: " + (end - start));
    System.exit(res);
  }

}
