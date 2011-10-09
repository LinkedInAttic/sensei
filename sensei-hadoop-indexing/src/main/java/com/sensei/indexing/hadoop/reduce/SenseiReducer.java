package com.sensei.indexing.hadoop.reduce;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Closeable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.log4j.Logger;

import com.sensei.indexing.hadoop.keyvalueformat.IntermediateForm;
import com.sensei.indexing.hadoop.keyvalueformat.Shard;
import com.sensei.indexing.hadoop.util.MRConfig;

public class SenseiReducer extends MapReduceBase implements
		Reducer<Shard, IntermediateForm, Shard, Text> {
	
	private static final Logger logger = Logger.getLogger(SenseiReducer.class);
	static final Text DONE = new Text("done");
	

	private Configuration iconf;
	private String mapredTempDir;
	  
    public void reduce(Shard key, Iterator<IntermediateForm> values,
                       OutputCollector<Shard, Text> output, 
                       Reporter reporter) throws IOException {

    	logger.info("Construct a shard writer for " + key);
        FileSystem fs = FileSystem.get(iconf);
        //debug:
        logger.info("filesystem is: "+ fs.getName());
        String temp =
            mapredTempDir + Path.SEPARATOR + "shard_" + System.currentTimeMillis();
        logger.info("mapredTempDir is: "+ mapredTempDir);
        final ShardWriter writer = new ShardWriter(fs, key, temp, iconf);

        // update the shard
        while (values.hasNext()) {
          IntermediateForm form = values.next();
          writer.process(form);
          reporter.progress();
        }

        // close the shard
        final Reporter fReporter = reporter;
        new Closeable() {
          volatile boolean closed = false;

          public void close() throws IOException {
            // spawn a thread to give progress heartbeats
            Thread prog = new Thread() {
              public void run() {
                while (!closed) {
                  try {
                    fReporter.setStatus("closing");
                    Thread.sleep(1000);
                  } catch (InterruptedException e) {
                    continue;
                  } catch (Throwable e) {
                    return;
                  }
                }
              }
            };

            try {
              prog.start();

              if (writer != null) {
//            	  writer.optimize();  //added this option to optimize after all the docs have been added;
                writer.close();
              }
            } finally {
              closed = true;
            }
          }
        }.close();
        logger.info("Closed the shard writer for " + key + ", writer = " + writer);
    	
    	
      output.collect(key, DONE);
    }
    
    
    public void configure(JobConf job) {
        iconf = job;
        mapredTempDir = iconf.get(MRConfig.TEMP_DIR);
        mapredTempDir = Shard.normalizePath(mapredTempDir);
      }
  }
