package com.senseidb.search.node.inmemory;

import java.io.File;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javax.management.MBeanServer;

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
import org.json.JSONException;
import org.json.JSONObject;

import proj.zoie.api.ZoieIndexReader;
import proj.zoie.api.ZoieMultiReader;
import proj.zoie.api.ZoieSegmentReader;
import proj.zoie.api.indexing.AbstractZoieIndexable;
import proj.zoie.api.indexing.ZoieIndexable;
import proj.zoie.impl.indexing.ZoieConfig;

import com.browseengine.bobo.api.BoboIndexReader;
import com.browseengine.bobo.facets.FacetHandler;
import com.browseengine.bobo.facets.RuntimeFacetHandlerFactory;
import com.senseidb.conf.SenseiConfParams;
import com.senseidb.conf.SenseiFacetHandlerBuilder;
import com.senseidb.conf.SenseiSchema;
import com.senseidb.conf.SenseiServerBuilder;
import com.senseidb.indexing.DefaultJsonSchemaInterpreter;
import com.senseidb.indexing.ShardingStrategy;
import com.senseidb.indexing.activity.ActivityPersistenceFactory;
import com.senseidb.jmx.JmxUtil;
import com.senseidb.jmx.MockJMXServer;
import com.senseidb.plugin.SenseiPluginRegistry;
import com.senseidb.search.node.SenseiIndexReaderDecorator;
import com.senseidb.search.plugin.PluggableSearchEngineManager;
import com.senseidb.search.req.SenseiRequest;
import com.senseidb.search.req.SenseiResult;
import com.senseidb.search.req.SenseiSystemInfo;
import com.senseidb.svc.impl.CoreSenseiServiceImpl;

public class InMemorySenseiService {
  private DefaultJsonSchemaInterpreter defaultJsonSchemaInterpreter;
  private List<FacetHandler<?>> facets;
  private List<RuntimeFacetHandlerFactory<?, ?>> runtimeFacets;

  private CoreSenseiServiceImpl coreSenseiServiceImpl;
  private PluggableSearchEngineManager pluggableSearchEngineManager;
  private MockSenseiCore mockSenseiCore;
  private SenseiSystemInfo senseiSystemInfo;
  private SenseiIndexReaderDecorator senseiIndexReaderDecorator;

  public InMemorySenseiService(SenseiSchema schema, SenseiPluginRegistry pluginRegistry) {
    MBeanServer platformMBeanServer = ManagementFactory.getPlatformMBeanServer();
    schema.setCompressSrcData(false);
    try {
      platformMBeanServer = JmxUtil.registerNewJmxServer(new MockJMXServer());
      defaultJsonSchemaInterpreter = new DefaultJsonSchemaInterpreter(schema);
      facets = new ArrayList<FacetHandler<?>>();
      runtimeFacets = new ArrayList<RuntimeFacetHandlerFactory<?, ?>>();

      ShardingStrategy strategy = new ShardingStrategy() {
        public int caculateShard(int maxShardId, JSONObject dataObj) throws JSONException {
          return 0;
        }
      };
      ActivityPersistenceFactory.setOverrideForCurrentThread(ActivityPersistenceFactory.getInMemoryInstance());
      pluggableSearchEngineManager = new PluggableSearchEngineManager();
      pluggableSearchEngineManager.init("", 0, schema, ZoieConfig.DEFAULT_VERSION_COMPARATOR, pluginRegistry, strategy);
      senseiSystemInfo = SenseiFacetHandlerBuilder.buildFacets(schema.getSchemaObj(), pluginRegistry, facets, runtimeFacets,
          pluggableSearchEngineManager);

      int[] partitions = new int[] { 0 };
       senseiIndexReaderDecorator = new SenseiIndexReaderDecorator(facets, runtimeFacets);
      mockSenseiCore = new MockSenseiCore(partitions, senseiIndexReaderDecorator);
      pluggableSearchEngineManager.start(mockSenseiCore);
      coreSenseiServiceImpl = new CoreSenseiServiceImpl(mockSenseiCore);
    } catch (Exception e) {
      throw new RuntimeException(e);
    } finally {
      JmxUtil.registerNewJmxServer(platformMBeanServer);
      ActivityPersistenceFactory.setOverrideForCurrentThread(null);

    }
  }

  private void addDocuments(Directory directory, IndexWriter writer, List<JSONObject> documents) {
    try {
      writer.deleteAll();
      for (JSONObject doc : documents) {
        if (doc == null)
          continue;
        writer.addDocument(buildDoc(doc));
        pluggableSearchEngineManager.update(doc, "");
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
      MockIndexReaderFactory mockIndexReaderFactory = new MockIndexReaderFactory<ZoieIndexReader<BoboIndexReader>>(
          Arrays.asList(zoieMultiReader));
      mockSenseiCore.setIndexReaderFactory(mockIndexReaderFactory);
      SenseiResult result = coreSenseiServiceImpl.execute(senseiRequest);
      mockSenseiCore.setIndexReaderFactory(null);
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
    ret.add(new Field(AbstractZoieIndexable.DOCUMENT_STORE_FIELD, indexable.getStoreValue()));
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

  public SenseiSystemInfo getSenseiSystemInfo() {
    return senseiSystemInfo;
  }

  public void setSenseiSystemInfo(SenseiSystemInfo senseiSystemInfo) {
    this.senseiSystemInfo = senseiSystemInfo;
  }
}
