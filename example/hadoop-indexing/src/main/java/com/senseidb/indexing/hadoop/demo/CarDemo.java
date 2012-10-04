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

package com.senseidb.indexing.hadoop.demo;



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
