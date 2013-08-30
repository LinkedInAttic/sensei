package com.senseidb.facet.handler.inverted;

import com.google.common.collect.Lists;
import com.senseidb.facet.FacetAccessible;
import com.senseidb.facet.FacetRequest;
import com.senseidb.facet.FacetRequestParams;
import com.senseidb.facet.FacetSpec;
import com.senseidb.facet.FacetSystem;
import com.senseidb.facet.handler.FacetHandler;
import com.senseidb.facet.search.FacetCollector;
import com.senseidb.facet.search.FacetMultiReader;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.FieldComparatorSource;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocsCollector;
import org.apache.lucene.search.TopFieldCollector;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.Version;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * @author Dmytro Ivchenko
 */
public class SimpleFacetHandlerTest {
  private RAMDirectory _dir;
  private FacetSystem _system;
  private SimpleFacetHandler _handler;
  private FacetMultiReader _reader;
  private IndexSearcher _searcher;

  @BeforeClass
  public void beforeClass() throws Exception {
    _dir = new RAMDirectory();
    IndexWriter writer = new IndexWriter(_dir, new IndexWriterConfig(Version.LUCENE_44, new WhitespaceAnalyzer(Version.LUCENE_44)));
    writer.addDocument(createDoc("red"));
    writer.addDocument(createDoc("red"));
    writer.addDocument(createDoc("green"));
    writer.addDocument(createDoc("blue"));
    writer.commit();

    _handler = new SimpleFacetHandler("color");
    _system = new FacetSystem(Lists.<FacetHandler<?>>newArrayList(_handler));

    _reader = _system.newReader( DirectoryReader.open(_dir) );
    _searcher = new IndexSearcher(_reader);
  }

  @Test
  public void test() throws Exception {
    // facet request
    FacetRequestParams params = new FacetRequestParams();
    params.setFacetSpec("color", new FacetSpec().setExpandSelection(true));
    FacetRequest request = new FacetRequest(_system, params);

    // sort
    FieldComparatorSource compSource = _handler.getFieldComparatorSource();
    Sort sort = new Sort(new SortField("color", compSource));
    TopDocsCollector topCollector = TopFieldCollector.create(sort, 3, false, false, false, true);
    FacetCollector facetCollector = request.newCollector(topCollector);

    // query
    BooleanQuery boolQuery = new BooleanQuery();
    boolQuery.add(new BooleanClause(new TermQuery(new Term("color", "red")), BooleanClause.Occur.SHOULD));
    boolQuery.add(new BooleanClause(new TermQuery(new Term("color", "green")), BooleanClause.Occur.SHOULD));

    _searcher.search(request.newQuery(boolQuery), facetCollector);

    // check counting
    FacetAccessible facet = facetCollector.getFacets().get("color");
    Assert.assertEquals(facet.getFacets().size(), 2);
    Assert.assertEquals(facet.getFacetHitsCount("red"), 2);
    Assert.assertEquals(facet.getFacetHitsCount("green"), 1);

    // check value retrieval and sorting
    Assert.assertEquals(3, topCollector.getTotalHits());
    ScoreDoc[] docs = topCollector.topDocs().scoreDocs;
    Assert.assertEquals(_reader.getFieldValue(request, docs[0].doc, "color"), "green");
    Assert.assertEquals(_reader.getFieldValue(request, docs[1].doc, "color"), "red");
  }


  private Document createDoc(String color)
  {
    Document doc = new Document();
    doc.add(new StringField("color", color, Field.Store.NO));
    return doc;
  }
}
