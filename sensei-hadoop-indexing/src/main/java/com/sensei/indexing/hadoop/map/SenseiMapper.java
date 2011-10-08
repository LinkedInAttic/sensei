package com.sensei.indexing.hadoop.map;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.net.URLConnection;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.util.Version;
import org.json.JSONException;
import org.json.JSONObject;

import proj.zoie.api.ZoieSegmentReader;
import proj.zoie.api.indexing.ZoieIndexable;
import proj.zoie.api.indexing.ZoieIndexable.IndexingReq;

import com.sensei.conf.SchemaConverter;
import com.sensei.conf.SenseiSchema;
import com.sensei.indexing.api.DefaultJsonSchemaInterpreter;
import com.sensei.indexing.api.JsonFilter;
import com.sensei.indexing.api.ShardingStrategy;
import com.sensei.indexing.hadoop.keyvalueformat.IntermediateForm;
import com.sensei.indexing.hadoop.keyvalueformat.Shard;

public class SenseiMapper extends MapReduceBase implements Mapper<Object, Object, Shard, IntermediateForm> {

	private final static Logger logger = Logger.getLogger(SenseiMapper.class);
	private static DefaultJsonSchemaInterpreter _defaultInterpreter = null;
	private boolean _use_remote_schema = false;
	private volatile boolean _isConfigured = false;
	private Configuration _conf;
	private Shard[] _shards;
	
	private ShardingStrategy _shardingStategy;
	private MapInputConverter _converter;
	private JsonFilter _filter;
	
	private Analyzer analyzer = new StandardAnalyzer(Version.LUCENE_30);
	  
    
    public void map(Object key, Object value, 
                    OutputCollector<Shard, IntermediateForm> output, 
                    Reporter reporter) throws IOException {
    	
        if(_isConfigured == false)
	      throw new IllegalStateException("Mapper's configure method wasn't sucessful. May not get the correct schema.");

        JSONObject json = null;
    	try{
    		json = _converter.getJsonInput(key, value);
    	}catch(JSONException e){
    		throw new IllegalStateException("data conversion failed inside mapper.");
    	}
    	
    	try{
    		json = _filter.filter(json);
    	}catch(Exception e){
    		throw new IllegalStateException("data filtering failed.");
    	}
    	
    	
    	if( _defaultInterpreter == null)
    		reporter.incrCounter("Map", "Interpreter_null", 1);
    	
    	if(  _defaultInterpreter != null && json != null){

    	      ZoieIndexable indexable = _defaultInterpreter.convertAndInterpret(json);
    	      
    	      IndexingReq[] idxReqs = indexable.buildIndexingReqs();
    	      if(idxReqs.length>0){
    	    	  Document doc = idxReqs[0].getDocument();
    		  	  ZoieSegmentReader.fillDocumentID(doc, indexable.getUID());
    	    	  
    		  	  //now we have uid and lucene Doc;
		          IntermediateForm form = new IntermediateForm();
		          form.configure(_conf);
		          form.process(doc, analyzer);
		          form.closeWriter();

		          int chosenShard = -1;
				  try {
					  chosenShard = _shardingStategy.caculateShard(_shards.length, json);
				  } catch (JSONException e) {
					  throw new IOException("sharding dose not work for mapper.");
				  }
		          if (chosenShard >= 0) {
		            // insert into one shard
		            output.collect(_shards[chosenShard], form);
		          } else {
		            throw new IOException("Chosen shard for insert must be >= 0");
		          }
    	      }
    	}
        
    }
    

	@Override
	public void configure(JobConf job) {
		super.configure(job);
		_conf = job;
	    _shards = Shard.getIndexShards(_conf);
		_use_remote_schema = job.getBoolean("schema.use.remote", false);
		
		_shardingStategy =
		        (ShardingStrategy) ReflectionUtils.newInstance(
				job.getClass("sea.distribution.policy",
				DummyShardingStrategy.class, ShardingStrategy.class), job);
		
		_converter = (MapInputConverter) ReflectionUtils.newInstance(
				job.getClass("sea.mapinput.converter",
						MapInputConverter.class, MapInputConverter.class), job);
		
		_filter = (JsonFilter) ReflectionUtils.newInstance(
				job.getClass("sea.mapinput.filter",
						DummyFilter.class, JsonFilter.class), job);
		
		try {
			getSchema(job, _use_remote_schema);
			_isConfigured = true;
		} catch (Exception e) {
			_isConfigured = false;
		}
    }

	private void getSchema(JobConf conf, boolean use_remote_schema) throws Exception {

		if (_use_remote_schema == true) {

			Path[] localFiles = DistributedCache.getLocalCacheFiles(conf);

			if (localFiles != null) {

				String _schema_uri = null;
				String metadataFileName = "";
				for (int i = 0; i < localFiles.length; i++) {
					String strFileName = localFiles[i].toString();
					if (strFileName.contains(conf.get("schema.file.url"))) {
						metadataFileName = strFileName;
						break;
					}
				}
				if (metadataFileName.length() > 0) {
					_schema_uri = "file:///" + metadataFileName;

					if (_defaultInterpreter == null) {
						
						logger.info("schema file is:" + _schema_uri);
						URL url = new URL(_schema_uri);
						URLConnection conn = url.openConnection();
						conn.connect();

						File xmlSchema = new File(url.toURI());
						if (!xmlSchema.exists()) {
							throw new ConfigurationException(
									"schema not file");
						}
						DocumentBuilderFactory dbf = DocumentBuilderFactory
								.newInstance();
						dbf.setIgnoringComments(true);
						DocumentBuilder db = dbf.newDocumentBuilder();
						org.w3c.dom.Document schemaXml = db
								.parse(xmlSchema);
						schemaXml.getDocumentElement().normalize();
						JSONObject schemaData = SchemaConverter
								.convert(schemaXml);

						SenseiSchema schema = SenseiSchema.build(schemaData);
						_defaultInterpreter = new DefaultJsonSchemaInterpreter(schema);
					}
				}
			}

		} else { // use local schema for debugging;
			String metadataFileName = conf.get("schema.file.local", "");
			if (metadataFileName.length() > 0) {
				if (_defaultInterpreter == null) {

					File xmlSchema = new File(metadataFileName);
					if (!xmlSchema.exists()) {
						throw new ConfigurationException("schema not file");
					}
					DocumentBuilderFactory dbf = DocumentBuilderFactory
							.newInstance();
					dbf.setIgnoringComments(true);
					DocumentBuilder db = dbf.newDocumentBuilder();
					org.w3c.dom.Document schemaXml = db.parse(xmlSchema);
					schemaXml.getDocumentElement().normalize();
					JSONObject schemaData = SchemaConverter
							.convert(schemaXml);

					SenseiSchema schema = SenseiSchema.build(schemaData);
					_defaultInterpreter = new DefaultJsonSchemaInterpreter(schema);
				}
			}

		}

			
	}

}
