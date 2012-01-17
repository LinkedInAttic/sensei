package com.senseidb.indexing;

import org.apache.lucene.document.Document;
import org.json.JSONObject;

import com.senseidb.conf.SenseiSchema;

public interface CustomIndexingPipeline{
  void applyCustomization(Document luceneDoc,SenseiSchema schema,JSONObject dataSource);
}
