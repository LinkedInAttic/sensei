package com.sensei.indexing.api;

import org.apache.lucene.document.Document;
import org.json.JSONObject;

import com.sensei.conf.SenseiSchema;

public interface CustomIndexingPipeline{
  void applyCustomization(Document luceneDoc,SenseiSchema schema,JSONObject dataSource);
}
