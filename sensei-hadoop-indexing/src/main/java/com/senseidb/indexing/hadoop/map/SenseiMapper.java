package com.senseidb.indexing.hadoop.map;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.net.URL;
import java.net.URLConnection;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.lang.exception.ExceptionUtils;
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
import org.apache.lucene.document.Field;
import org.apache.lucene.util.Version;
import org.json.JSONException;
import org.json.JSONObject;

import com.linkedin.zoie.api.ZoieSegmentReader;
import com.linkedin.zoie.api.indexing.AbstractZoieIndexable;
import com.linkedin.zoie.api.indexing.ZoieIndexable;
import com.linkedin.zoie.api.indexing.ZoieIndexable.IndexingReq;

import com.senseidb.conf.SchemaConverter;
import com.senseidb.conf.SenseiSchema;
import com.senseidb.indexing.DefaultJsonSchemaInterpreter;
import com.senseidb.indexing.JsonFilter;
import com.senseidb.indexing.ShardingStrategy;
import com.senseidb.indexing.hadoop.keyvalueformat.IntermediateForm;
import com.senseidb.indexing.hadoop.keyvalueformat.Shard;
import com.senseidb.indexing.hadoop.util.SenseiJobConfig;

public class SenseiMapper extends MapReduceBase implements Mapper<Object, Object, Shard, IntermediateForm> {

	private final static Logger logger = Logger.getLogger(SenseiMapper.class);
	private static DefaultJsonSchemaInterpreter _defaultInterpreter = null;
	private boolean _use_remote_schema = false;
	private volatile boolean _isConfigured = false;
	private Configuration _conf;
	private Shard[] _shards;
	
	private ShardingStrategy _shardingStategy;
	private MapInputConverter _converter;
	
	private static Analyzer analyzer;
	

	  
    
    public void map(Object key, Object value, 
                    OutputCollector<Shard, IntermediateForm> output, 
                    Reporter reporter) throws IOException {
    	
        if(_isConfigured == false)
	      throw new IllegalStateException("Mapper's configure method wasn't sucessful. May not get the correct schema or Lucene Analyzer.");

        JSONObject json = null;
    	try{
    		json = _converter.getJsonInput(key, value, _conf);
    		json = _converter.doFilter(json);
    	}catch(Exception e){
    		ExceptionUtils.printRootCauseStackTrace(e);
    		throw new IllegalStateException("data conversion or filtering failed inside mapper. \n");
    	}
    	
    	
    	if( _defaultInterpreter == null)
    		reporter.incrCounter("Map", "Interpreter_null", 1);
    	
    	if(  _defaultInterpreter != null && json != null && analyzer != null){

    	      ZoieIndexable indexable = _defaultInterpreter.convertAndInterpret(json);
    	      
    	      IndexingReq[] idxReqs = indexable.buildIndexingReqs();
    	      if(idxReqs.length>0){
    	    	  Document doc = idxReqs[0].getDocument();
    		  	  ZoieSegmentReader.fillDocumentID(doc, indexable.getUID());
    	    	  
                  if (indexable.isStorable()){
                    byte[] bytes = indexable.getStoreValue();
                    if (bytes!=null){
                      doc.add(new Field(AbstractZoieIndexable.DOCUMENT_STORE_FIELD,bytes));
                    }
                  }
    		  	  
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
		            throw new IOException("Chosen shard for insert must be >= 0. current shard is: " + chosenShard);
		          }
    	      }
    	}
        
    }
    

	@Override
	public void configure(JobConf job) {
		super.configure(job);
		_conf = job;
	    _shards = Shard.getIndexShards(_conf);
		
		_shardingStategy =
		        (ShardingStrategy) ReflectionUtils.newInstance(
				job.getClass(SenseiJobConfig.DISTRIBUTION_POLICY,
				DummyShardingStrategy.class, ShardingStrategy.class), job);
		
		_converter = (MapInputConverter) ReflectionUtils.newInstance(
				job.getClass(SenseiJobConfig.MAPINPUT_CONVERTER,
						DummyMapInputConverter.class, MapInputConverter.class), job);
		
		try {
			setSchema(job);
			setAnalyzer(job);		   	 
			
			_isConfigured = true;
		} catch (Exception e) {
			e.printStackTrace();
			_isConfigured = false;
		}
    }
	
	private void setAnalyzer(JobConf conf) throws Exception{
		
		if(analyzer != null)
			return;
		
		String version = _conf.get(SenseiJobConfig.DOCUMENT_ANALYZER_VERSION);
		if(version == null)
			 throw new IllegalStateException("version has not been specified");
		
		String analyzerName = _conf.get(SenseiJobConfig.DOCUMENT_ANALYZER);
		if(analyzerName == null)
			 throw new IllegalStateException("analyzer name has not been specified");
		
		Class analyzerClass = Class.forName(analyzerName);
		Constructor constructor = analyzerClass.getConstructor(Version.class);
		analyzer = (Analyzer) constructor.newInstance((Version) Enum.valueOf((Class)Class.forName("org.apache.lucene.util.Version"), version));

	}

	private void setSchema(JobConf conf) throws Exception {

		String _schema_uri = null;
		String metadataFileName = conf.get(SenseiJobConfig.SCHEMA_FILE_URL);
		
		Path[] localFiles = DistributedCache.getLocalCacheFiles(conf);
		if (localFiles != null) {
		  for (int i = 0; i < localFiles.length; i++) {
			  String strFileName = localFiles[i].toString();
			  if (strFileName.contains(conf.get(SenseiJobConfig.SCHEMA_FILE_URL))) {
				  metadataFileName = strFileName;
				  break;
			  }
		  }
		}
		
		if (metadataFileName != null && metadataFileName.length() > 0) {
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

}
