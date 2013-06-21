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
package com.senseidb.indexing.hadoop.reduce;

import java.io.File;
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

import com.senseidb.indexing.hadoop.keyvalueformat.IntermediateForm;
import com.senseidb.indexing.hadoop.keyvalueformat.Shard;
import com.senseidb.indexing.hadoop.util.MRConfig;

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
        String temp = mapredTempDir + Path.SEPARATOR + "shard_" + key.toFlatString()+ "_" + System.currentTimeMillis();
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
            	writer.optimize();  //added this option to optimize after all the docs have been added;
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
    
    @Override
    public void close() throws IOException {
    	if(mapredTempDir != null){
    		File file = new File(mapredTempDir);
    		if(file.exists())
    			deleteDir(file);
    	}
    }
    
    static void deleteDir(File file)
    {
      if (file == null || !file.exists())
        return;
      for (File f : file.listFiles())
      {
        if (f.isDirectory())
          deleteDir(f);
        else
          f.delete();
      }
      file.delete();
    }
    
    public void configure(JobConf job) {
        iconf = job;
        mapredTempDir = iconf.get(MRConfig.TEMP_DIR);
        mapredTempDir = Shard.normalizePath(mapredTempDir);
      }
  }
