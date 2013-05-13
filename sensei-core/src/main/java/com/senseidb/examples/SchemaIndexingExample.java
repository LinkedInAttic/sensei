package com.senseidb.examples;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.HashSet;
import java.util.Set;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriter.MaxFieldLength;
import org.apache.lucene.store.SimpleFSDirectory;
import org.apache.lucene.util.Version;
import org.json.JSONObject;

import proj.zoie.api.indexing.ZoieIndexable;
import proj.zoie.api.indexing.ZoieIndexable.IndexingReq;

import com.senseidb.conf.SenseiSchema;
import com.senseidb.conf.SenseiServerBuilder;
import com.senseidb.indexing.DefaultJsonSchemaInterpreter;
import com.senseidb.search.plugin.PluggableSearchEngineManager;
import com.senseidb.util.JSONUtil.FastJSONArray;
import com.senseidb.util.JSONUtil.FastJSONObject;

public class SchemaIndexingExample{
  
  public static void main(String[] args) throws Exception{
    File confDir = new File(args[0]);
    File dataFile = new File(args[1]);
    File idxDir = new File(args[2]);
    
    JSONObject schemaData = SenseiServerBuilder.loadSchema(confDir);
    SenseiSchema schema = SenseiSchema.build(schemaData);
    
    DefaultJsonSchemaInterpreter defaultInterpreter = new DefaultJsonSchemaInterpreter(schema, new PluggableSearchEngineManager() {
      @Override
      public Set<String> getFieldNames() {
        return new HashSet<String>();
      }
    });
    
    FileReader freader = new FileReader(dataFile);
    BufferedReader br = new BufferedReader(freader);
    IndexWriter idxWriter = new IndexWriter(SimpleFSDirectory.open(idxDir),new StandardAnalyzer(Version.LUCENE_CURRENT),MaxFieldLength.UNLIMITED);
    while(true){
      String line = br.readLine();
      if (line==null) break;
      
      JSONObject obj = new FastJSONObject(line);
      ZoieIndexable indexable = defaultInterpreter.convertAndInterpret(obj);
      IndexingReq[] idxReqs = indexable.buildIndexingReqs();
      for (IndexingReq req : idxReqs){
        Document doc = req.getDocument();
        idxWriter.addDocument(doc);
      }
      
      idxWriter.commit();
      idxWriter.optimize();
      idxWriter.close();
    }
    freader.close();
  }
}
