package com.senseidb.search.node.inmemory;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.Version;
import org.json.JSONObject;

import proj.zoie.api.ZoieIndexReader;
import proj.zoie.api.ZoieMultiReader;
import proj.zoie.api.ZoieSegmentReader;
import proj.zoie.api.indexing.AbstractZoieIndexable;
import proj.zoie.api.indexing.ZoieIndexable;

import com.browseengine.bobo.api.BoboIndexReader;
import com.browseengine.bobo.facets.FacetHandler;
import com.browseengine.bobo.facets.RuntimeFacetHandlerFactory;
import com.senseidb.conf.SenseiFacetHandlerBuilder;
import com.senseidb.conf.SenseiSchema;
import com.senseidb.conf.SenseiServerBuilder;
import com.senseidb.indexing.DefaultJsonSchemaInterpreter;
import com.senseidb.plugin.SenseiPluginRegistry;
import com.senseidb.search.node.SenseiIndexReaderDecorator;
import com.senseidb.search.plugin.PluggableSearchEngineManager;
import com.senseidb.search.req.SenseiRequest;
import com.senseidb.search.req.SenseiResult;
import com.senseidb.svc.impl.CoreSenseiServiceImpl;

public class InMemorySenseiService {
  private DefaultJsonSchemaInterpreter defaultJsonSchemaInterpreter;
  private List<FacetHandler<?>> facets;
  private List<RuntimeFacetHandlerFactory<?, ?>> runtimeFacets;
  private MockIndexReaderFactory<ZoieIndexReader<BoboIndexReader>> mockIndexReaderFactory;
  private CoreSenseiServiceImpl coreSenseiServiceImpl;

  public InMemorySenseiService(SenseiSchema schema, SenseiPluginRegistry pluginRegistry) {    
    schema.setCompressSrcData(false);
    try {
      defaultJsonSchemaInterpreter = new DefaultJsonSchemaInterpreter(schema);    
      facets = new ArrayList<FacetHandler<?>>();
      runtimeFacets = new ArrayList<RuntimeFacetHandlerFactory<?, ?>>();
      SenseiFacetHandlerBuilder.buildFacets(schema.getSchemaObj(), pluginRegistry, facets, runtimeFacets,
          new PluggableSearchEngineManager());
      mockIndexReaderFactory = new MockIndexReaderFactory<ZoieIndexReader<BoboIndexReader>>();
      MockSenseiCore mockSenseiCore = new MockSenseiCore(mockIndexReaderFactory);
      coreSenseiServiceImpl = new CoreSenseiServiceImpl(mockSenseiCore); 
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private void addDocuments(Directory directory, IndexWriter writer, List<JSONObject> documents) {
    try {
      writer.deleteAll();
      for (JSONObject doc : documents) {
        if (doc == null)
          continue;
        writer.addDocument(buildDoc(doc));
      }
      writer.commit();

    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @SuppressWarnings("unchecked")
  public SenseiResult doQuery(SenseiRequest senseiRequest, List<JSONObject> documents) {
    Directory directory = null;
    IndexWriter writer = null;
    try {
      directory = new RAMDirectory();
      writer = new IndexWriter(directory, new IndexWriterConfig(Version.LUCENE_35, new StandardAnalyzer(Version.LUCENE_35)));
      addDocuments(directory, writer, documents);
      ZoieIndexReader<BoboIndexReader> zoieMultiReader = new ZoieMultiReader<BoboIndexReader>(IndexReader.open(directory),
          new SenseiIndexReaderDecorator(facets, runtimeFacets));
      mockIndexReaderFactory.setIndexReadersForCurrentThread(Arrays.asList(zoieMultiReader));
      SenseiResult result = coreSenseiServiceImpl.execute(senseiRequest);
      mockIndexReaderFactory.setIndexReadersForCurrentThread(Collections.EMPTY_LIST);
      return result;
    } catch (Exception e) {
      throw new RuntimeException(e);
    } finally {
      try {
        if (writer != null) {
          writer.close();
        }
        if (directory != null) {
          directory.close();
        }
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

  public Document buildDoc(JSONObject json) {
    ZoieIndexable indexable = defaultJsonSchemaInterpreter.convertAndInterpret(json);
    Document ret = indexable.buildIndexingReqs()[0].getDocument();
    ret.add(new Field(AbstractZoieIndexable.DOCUMENT_STORE_FIELD,indexable.getStoreValue()));
    ZoieSegmentReader.fillDocumentID(ret, indexable.getUID());
    return ret;
  }

  public static InMemorySenseiService valueOf(File confDir) {
    try {
      JSONObject schema = SenseiServerBuilder.loadSchema(confDir);
      File senseiConfFile = new File(confDir, SenseiServerBuilder.SENSEI_PROPERTIES);
      if (!senseiConfFile.exists()) {
        throw new ConfigurationException("configuration file: " + senseiConfFile.getAbsolutePath() + " does not exist.");
      }
      Configuration senseiConf = new PropertiesConfiguration();
      ((PropertiesConfiguration) senseiConf).setDelimiterParsingDisabled(true);
      ((PropertiesConfiguration) senseiConf).load(senseiConfFile);
      return new InMemorySenseiService(SenseiSchema.build(schema), SenseiPluginRegistry.build(senseiConf));
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }
}
