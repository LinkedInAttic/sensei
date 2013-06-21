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
