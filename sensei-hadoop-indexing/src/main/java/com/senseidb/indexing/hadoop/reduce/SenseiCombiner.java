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

import java.io.IOException;
import java.util.Iterator;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.log4j.Logger;

import com.senseidb.indexing.hadoop.keyvalueformat.IntermediateForm;
import com.senseidb.indexing.hadoop.keyvalueformat.Shard;
import com.senseidb.indexing.hadoop.util.SenseiJobConfig;

/**
 * This combiner combines multiple intermediate forms into one intermediate
 * form. More specifically, the input intermediate forms are a single-document
 * ram index and/or a single delete term. An output intermediate form contains
 * a multi-document ram index and/or multiple delete terms.   
 */
public class SenseiCombiner extends MapReduceBase implements
    Reducer<Shard, IntermediateForm, Shard, IntermediateForm> {
	
  private static final Logger logger = Logger.getLogger(SenseiCombiner.class);	

  Configuration iconf;
  long maxSizeInBytes;
  long nearMaxSizeInBytes;


  public void reduce(Shard key, Iterator<IntermediateForm> values,
      OutputCollector<Shard, IntermediateForm> output, Reporter reporter)
      throws IOException {

    String message = key.toString();
    IntermediateForm form = null;

    while (values.hasNext()) {
      IntermediateForm singleDocForm = values.next();
      long formSize = form == null ? 0 : form.totalSizeInBytes();
      long singleDocFormSize = singleDocForm.totalSizeInBytes();

      if (form != null && formSize + singleDocFormSize > maxSizeInBytes) {
        closeForm(form, message);
        output.collect(key, form);
        form = null;
      }

      if (form == null && singleDocFormSize >= nearMaxSizeInBytes) {
        output.collect(key, singleDocForm);
      } else {
        if (form == null) {
          form = createForm(message);
        }
        form.process(singleDocForm);
      }
    }

    if (form != null) {
      closeForm(form, message);
      output.collect(key, form);
    }
  }

  private IntermediateForm createForm(String message) throws IOException {
	logger.info("Construct a form writer for " + message);
    IntermediateForm form = new IntermediateForm();
    form.configure(iconf);
    return form;
  }

  private void closeForm(IntermediateForm form, String message)
      throws IOException {
    form.closeWriter();
    logger.info("Closed the form writer for " + message + ", form = " + form);
  }


  public void configure(JobConf job) {
    iconf = new Configuration(job);
    maxSizeInBytes = iconf.getLong(SenseiJobConfig.MAX_RAMSIZE_BYTES, 50L << 20);
    nearMaxSizeInBytes = maxSizeInBytes - (maxSizeInBytes >>> 3); // 7/8 of max
  }

  public void close() throws IOException {
  }

}
