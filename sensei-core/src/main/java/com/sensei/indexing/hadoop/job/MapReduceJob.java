package com.sensei.indexing.hadoop.job;

import java.io.IOException;
import java.text.NumberFormat;
import java.util.Arrays;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.util.StringUtils;

import com.sensei.indexing.hadoop.keyvalueformat.IntermediateForm;
import com.sensei.indexing.hadoop.keyvalueformat.Shard;
import com.sensei.indexing.hadoop.map.SenseiMapper;
import com.sensei.indexing.hadoop.reduce.FileSystemDirectory;
import com.sensei.indexing.hadoop.reduce.IndexUpdateOutputFormat;
import com.sensei.indexing.hadoop.reduce.SenseiCombiner;
import com.sensei.indexing.hadoop.reduce.SenseiReducer;
import com.sensei.indexing.hadoop.util.LuceneUtil;
import com.sensei.indexing.hadoop.util.MRJobConfig;

public class MapReduceJob extends Configured {

	private static final NumberFormat NUMBER_FORMAT = NumberFormat.getInstance();
	public static final Log LOG = LogFactory.getLog(MapReduceJob.class);
	
	  public JobConf createJob(Class MRClass) throws IOException {
		    
		    Configuration conf = getConf();
		    Path[] inputPaths;
		    Path outputPath;
		    Shard[] shards = null;
			int numMapTasks = conf.getInt("mapreduce.job.maps", 2);
			int numShards = conf.getInt("sea.num.shards", 2);
//			inputPaths = FileInputFormat.getInputPaths(jobConf);
			
		    String dirs = conf.get("mapreduce.input.fileinputformat.inputdir", null);
		    LOG.info("dirs:"+ dirs);
		    String [] list = StringUtils.split(dirs);
		    LOG.info("length after split:"+ list.length);
		    inputPaths = new Path[list.length];
		    for (int i = 0; i < list.length; i++) {
		    	inputPaths[i] = new Path(StringUtils.unEscapeString(list[i]));
		    }
		    LOG.info("path[0] is:" + inputPaths[0]);
		    	    
			outputPath = new Path(conf.get("mapreduce.output.fileoutputformat.outputdir"));
			String indexPath = conf.get("sea.index.path");
			shards = createShards(indexPath, numShards, conf);
			
			
		    // set the starting generation for each shard
		    // when a reduce task fails, a new reduce task
		    // has to know where to re-start
		    setShardGeneration(conf, shards);

		    Shard.setIndexShards(conf, shards);

		    // MapTask.MapOutputBuffer uses JobContext.IO_SORT_MB to decide its max buffer size
		    // (max buffer size = 1/2 * JobContext.IO_SORT_MB).
		    // Here we half-en JobContext.IO_SORT_MB because we use the other half memory to
		    // build an intermediate form/index in Combiner.
		    conf.setInt(MRJobConfig.IO_SORT_MB,  conf.getInt(MRJobConfig.IO_SORT_MB, 100) / 2);

		    // create the job configuration
		    JobConf jobConf = new JobConf(conf, MRClass);
		    jobConf.setJobName(MRClass.getName() + "_"+ System.currentTimeMillis());

		    // provided by application
		    FileInputFormat.setInputPaths(jobConf, inputPaths);
		    FileOutputFormat.setOutputPath(jobConf, outputPath);

		    jobConf.setNumMapTasks(numMapTasks);

		    // already set shards
		    jobConf.setNumReduceTasks(shards.length);

		    jobConf.setInputFormat(
		    		conf.getClass("sea.input.format", TextInputFormat.class, InputFormat.class));

		    Path[] inputs = FileInputFormat.getInputPaths(jobConf);
		    StringBuilder buffer = new StringBuilder(inputs[0].toString());
		    for (int i = 1; i < inputs.length; i++) {
		      buffer.append(",");
		      buffer.append(inputs[i].toString());
		    }
		    LOG.info("mapred.input.dir = " + buffer.toString());
		    LOG.info("mapreduce.output.fileoutputformat.outputdir = " + 
		             FileOutputFormat.getOutputPath(jobConf).toString());
		    LOG.info("mapreduce.job.maps = " + jobConf.getNumMapTasks());
		    LOG.info("mapreduce.job.reduces = " + jobConf.getNumReduceTasks());
		    LOG.info(shards.length + " shards = " + conf.get("sea.index.shards"));
		    LOG.info("mapred.input.format.class = "
		        + jobConf.getInputFormat().getClass().getName());

		    // set by the system
		    jobConf.setMapOutputKeyClass(Shard.class);
		    jobConf.setMapOutputValueClass(IntermediateForm.class);
		    jobConf.setOutputKeyClass(Shard.class);
		    jobConf.setOutputValueClass(Text.class);

		    jobConf.setMapperClass(SenseiMapper.class);
//		    jobConf.setPartitionerClass(IndexUpdatePartitioner.class);
		    jobConf.setCombinerClass(SenseiCombiner.class);
		    jobConf.setReducerClass(SenseiReducer.class);

		    jobConf.setOutputFormat(IndexUpdateOutputFormat.class);

		    return jobConf;
		  }
	  
	  
	  
	  private static Shard[] createShards(String indexPath, int numShards,
			  org.apache.hadoop.conf.Configuration conf) throws IOException {

	    String parent = Shard.normalizePath(indexPath) + Path.SEPARATOR;
	    long versionNumber = -1;
	    long generation = -1;

	    FileSystem fs = FileSystem.get(conf);
	    Path path = new Path(indexPath);

	    if (fs.exists(path)) {
	      FileStatus[] fileStatus = fs.listStatus(path);
	      String[] shardNames = new String[fileStatus.length];
	      int count = 0;
	      for (int i = 0; i < fileStatus.length; i++) {
	        if (fileStatus[i].isDir()) {
	          shardNames[count] = fileStatus[i].getPath().getName();
	          count++;
	        }
	      }
	      Arrays.sort(shardNames, 0, count);

	      Shard[] shards = new Shard[count >= numShards ? count : numShards];
	      for (int i = 0; i < count; i++) {
	        shards[i] =
	            new Shard(versionNumber, parent + shardNames[i], generation);
	      }

	      int number = count;
	      for (int i = count; i < numShards; i++) {
	        String shardPath;
	        while (true) {
	          shardPath = parent + NUMBER_FORMAT.format(number++);
	          if (!fs.exists(new Path(shardPath))) {
	            break;
	          }
	        }
	        shards[i] = new Shard(versionNumber, shardPath, generation);
	      }
	      return shards;
	    } else {
	      Shard[] shards = new Shard[numShards];
	      for (int i = 0; i < shards.length; i++) {
	        shards[i] =
	            new Shard(versionNumber, parent + NUMBER_FORMAT.format(i),
	                generation);
	      }
	      return shards;
	    }
	  }
	  
	  
	  void setShardGeneration(Configuration conf, Shard[] shards)
		      throws IOException {
		    FileSystem fs = FileSystem.get(conf);

		    for (int i = 0; i < shards.length; i++) {
		      Path path = new Path(shards[i].getDirectory());
		      long generation = -1;

		      if (fs.exists(path)) {
		        FileSystemDirectory dir = null;

		        try {
		          dir = new FileSystemDirectory(fs, path, false, conf);
		          generation = LuceneUtil.getCurrentSegmentGeneration(dir);
		        } finally {
		          if (dir != null) {
		            dir.close();
		          }
		        }
		      }

		      if (generation != shards[i].getGeneration()) {
		        // set the starting generation for the shard
		        shards[i] =
		            new Shard(shards[i].getVersion(), shards[i].getDirectory(),
		                generation);
		      }
		    }
		  }
}
