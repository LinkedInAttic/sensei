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
