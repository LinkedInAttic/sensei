package com.sensei.indexing.hadoop.demo;



import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.sensei.indexing.hadoop.job.MapReduceJob;
import com.sensei.indexing.hadoop.util.PropertiesLoader;


public class CarDemo extends MapReduceJob implements Tool {
  

  public int run(String[] args) throws Exception {
    JobConf conf = createJob(CarDemo.class);
    
    conf.setJobName("CarDemo");
    JobClient.runJob(conf);
    return 0;
  }
  

  public static void main(String[] args) throws Exception {
	Configuration conf = PropertiesLoader.loadProperties("example/hadoop-indexing/conf/JobCarDemo.job");
    int res = ToolRunner.run(conf, new CarDemo(), new String[]{});
    System.exit(res);
  }

}