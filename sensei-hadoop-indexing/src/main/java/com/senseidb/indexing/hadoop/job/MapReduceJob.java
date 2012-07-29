package com.senseidb.indexing.hadoop.job;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.NumberFormat;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Trash;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.util.StringUtils;
import org.apache.log4j.Logger;

import com.senseidb.indexing.hadoop.keyvalueformat.IntermediateForm;
import com.senseidb.indexing.hadoop.keyvalueformat.Shard;
import com.senseidb.indexing.hadoop.map.SenseiMapper;
import com.senseidb.indexing.hadoop.reduce.FileSystemDirectory;
import com.senseidb.indexing.hadoop.reduce.IndexUpdateOutputFormat;
import com.senseidb.indexing.hadoop.reduce.SenseiCombiner;
import com.senseidb.indexing.hadoop.reduce.SenseiReducer;
import com.senseidb.indexing.hadoop.util.LuceneUtil;
import com.senseidb.indexing.hadoop.util.MRConfig;
import com.senseidb.indexing.hadoop.util.MRJobConfig;
import com.senseidb.indexing.hadoop.util.SenseiJobConfig;

public class MapReduceJob extends Configured {

	private static final NumberFormat NUMBER_FORMAT = NumberFormat.getInstance();
	private static final Logger logger = Logger.getLogger(MapReduceJob.class);
	
	  public JobConf createJob(Class MRClass) throws IOException, URISyntaxException {
		    
		    Configuration conf = getConf();
		    Path[] inputPaths;
		    Path outputPath;
		    Shard[] shards = null;
			int numMapTasks = conf.getInt(MRJobConfig.NUM_MAPS, 2);
			int numShards = conf.getInt(SenseiJobConfig.NUM_SHARDS, 2);
//			inputPaths = FileInputFormat.getInputPaths(jobConf);
			
		    String dirs = conf.get(SenseiJobConfig.INPUT_DIRS, null);
		    logger.info("dirs:"+ dirs);
		    String [] list = StringUtils.split(dirs);
		    logger.info("length after split:"+ list.length);
		    inputPaths = new Path[list.length];
		    for (int i = 0; i < list.length; i++) {
		    	inputPaths[i] = new Path(StringUtils.unEscapeString(list[i]));
		    }
		    logger.info("path[0] is:" + inputPaths[0]);
		    	    
			outputPath = new Path(conf.get(SenseiJobConfig.OUTPUT_DIR));
			String indexPath = conf.get(SenseiJobConfig.INDEX_PATH);
			String indexSubDirPrefix = conf.get(SenseiJobConfig.INDEX_SUBDIR_PREFIX, "");
			shards = createShards(indexPath, numShards, conf, indexSubDirPrefix);
			
		    FileSystem fs = FileSystem.get(conf);
		    String username = conf.get("hadoop.job.ugi");
		    if (fs.exists(outputPath) && conf.getBoolean(SenseiJobConfig.FORCE_OUTPUT_OVERWRITE, false))
		        fs.delete(outputPath, true);
		    if (fs.exists(new Path(indexPath)) && conf.getBoolean(SenseiJobConfig.FORCE_OUTPUT_OVERWRITE, false))
		        fs.delete(new Path(indexPath), true);
			
			
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
		    
		    // set the temp dir for the job;
		    conf.set(MRConfig.TEMP_DIR, "${mapred.child.tmp}/hindex/");
		    if (fs.exists(new Path(conf.get(MRConfig.TEMP_DIR))))
		        fs.delete(new Path(conf.get(MRConfig.TEMP_DIR)), true);
		    if(fs.exists(new Path("./tmp")))
		    	fs.delete(new Path("./tmp"), true);
		    
		    (new Trash(conf)).expunge();  //empty trash;
		    
		    
		    //always using compound file format to speed up;
		    conf.setBoolean(SenseiJobConfig.USE_COMPOUND_FILE, true);
		    
		    String schemaFile = conf.get(SenseiJobConfig.SCHEMA_FILE_URL);
		    if(schemaFile == null)
		    	throw new IOException("no schema file is found");
		    else{
		    	logger.info("Adding schema file: " + conf.get(SenseiJobConfig.SCHEMA_FILE_URL));	      
				DistributedCache.addCacheFile(new URI(schemaFile), conf);
		    }

		    // create the job configuration
		    JobConf jobConf = new JobConf(conf, MRClass);
		    if(jobConf.getJobName().length()<1)
		    	jobConf.setJobName(MRClass.getName() + "_"+ System.currentTimeMillis());

		    // provided by application
		    FileInputFormat.setInputPaths(jobConf, inputPaths);
		    FileOutputFormat.setOutputPath(jobConf, outputPath);

		    jobConf.setNumMapTasks(numMapTasks);

		    // already set shards
		    jobConf.setNumReduceTasks(shards.length);

		    jobConf.setInputFormat(
		    		conf.getClass(SenseiJobConfig.INPUT_FORMAT, TextInputFormat.class, InputFormat.class));

		    Path[] inputs = FileInputFormat.getInputPaths(jobConf);
		    StringBuilder buffer = new StringBuilder(inputs[0].toString());
		    for (int i = 1; i < inputs.length; i++) {
		      buffer.append(",");
		      buffer.append(inputs[i].toString());
		    }
		    logger.info("mapred.input.dir = " + buffer.toString());
		    logger.info("mapreduce.output.fileoutputformat.outputdir = " + 
		             FileOutputFormat.getOutputPath(jobConf).toString());
		    logger.info("mapreduce.job.maps = " + jobConf.getNumMapTasks());
		    logger.info("mapreduce.job.reduces = " + jobConf.getNumReduceTasks());
		    logger.info(shards.length + " shards = " + conf.get(SenseiJobConfig.INDEX_SHARDS));
		    logger.info("mapred.input.format.class = "
		        + jobConf.getInputFormat().getClass().getName());
		    logger.info("mapreduce.cluster.temp.dir = " + jobConf.get(MRConfig.TEMP_DIR));

		    // set by the system
		    jobConf.setMapOutputKeyClass(Shard.class);
		    jobConf.setMapOutputValueClass(IntermediateForm.class);
		    jobConf.setOutputKeyClass(Shard.class);
		    jobConf.setOutputValueClass(Text.class);

		    jobConf.setMapperClass(SenseiMapper.class);
		    // no need for the partitioner.class;
		    jobConf.setCombinerClass(SenseiCombiner.class);
		    jobConf.setReducerClass(SenseiReducer.class);

		    jobConf.setOutputFormat(IndexUpdateOutputFormat.class);

		    jobConf.setReduceSpeculativeExecution(false);
		    return jobConf;
		  }
	  
	  private static FileSystem getFileSystem(String user) {
		    Configuration conf = new Configuration();
		    conf.set("hadoop.job.ugi", user);
			try
			{
		      return FileSystem.get(conf);
		    }
		    catch(IOException e)
		    {
		      throw new RuntimeException(e);    
		    }
		  }
	  
	  private static Shard[] createShards(String indexPath, int numShards,
			  org.apache.hadoop.conf.Configuration conf, String indexSubDirPrefix) throws IOException {

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
	          shardPath = parent + indexSubDirPrefix + NUMBER_FORMAT.format(number++);
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
	            new Shard(versionNumber, parent + indexSubDirPrefix + NUMBER_FORMAT.format(i),
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
