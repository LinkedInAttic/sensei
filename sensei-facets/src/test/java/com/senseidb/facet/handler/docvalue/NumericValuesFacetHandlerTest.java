package com.senseidb.facet.handler.docvalue;

import com.google.common.collect.Lists;
import com.senseidb.facet.Facet;
import com.senseidb.facet.FacetCollector;
import com.senseidb.facet.FacetMultiReader;
import com.senseidb.facet.FacetRequest;
import com.senseidb.facet.FacetRequestParams;
import com.senseidb.facet.FacetSpec;
import com.senseidb.facet.FacetSystem;
import com.senseidb.facet.handler.FacetHandler;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FieldInfo;
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

import java.util.List;

/**
 * @author Dmytro Ivchenko
 */
public class NumericValuesFacetHandlerTest {
  private RAMDirectory _dir;
  private FacetSystem _system;
  private NumericValuesFacetHandler _handler;
  private FacetMultiReader _reader;
  private IndexSearcher _searcher;

  @BeforeClass
  public void beforeClass() throws Exception {
    _dir = new RAMDirectory();
    IndexWriter writer = new IndexWriter(_dir, new IndexWriterConfig(Version.LUCENE_44, new WhitespaceAnalyzer(Version.LUCENE_44)));
    writer.addDocument(createDoc(2005));
    writer.addDocument(createDoc(2005));
    writer.addDocument(createDoc(2003));
    writer.addDocument(createDoc(2007));
    writer.commit();

    _handler = new NumericValuesFacetHandler("year");
    _system = new FacetSystem(Lists.<FacetHandler>newArrayList(_handler));

    _reader = new FacetMultiReader(_system, DirectoryReader.open(_dir));
    _searcher = new IndexSearcher(_reader);
  }

  @Test
  public void test() throws Exception {
    // _facet request
    FacetRequestParams params = new FacetRequestParams();
    params.setFacetSpec("year", new FacetSpec().setExpandSelection(true).setOrderBy(FacetSpec.FacetSortSpec.OrderHitsDesc));
    FacetRequest request = new FacetRequest(_system, params);

    // sort
    FieldComparatorSource compSource = _handler.getFieldComparatorSource();
    Sort sort = new Sort(new SortField("year", compSource, true));
    TopDocsCollector topCollector = TopFieldCollector.create(sort, 3, false, false, false, true);
    FacetCollector facetCollector = request.newCollector(topCollector);

    // query
    BooleanQuery boolQuery = new BooleanQuery();
    boolQuery.add(new BooleanClause(new TermQuery(new Term("year", "2005")), BooleanClause.Occur.SHOULD));
    boolQuery.add(new BooleanClause(new TermQuery(new Term("year", "2003")), BooleanClause.Occur.SHOULD));

    _searcher.search(request.newQuery(boolQuery), facetCollector);

    // check counting
    List<Facet> facets = facetCollector.getFacets().get("year").getTopFacets();
    Assert.assertEquals(facets.size(), 2);
    Assert.assertEquals(facets.get(0).getFacetValueHitCount(), 2);
    Assert.assertEquals(facets.get(0).getValue(), "2005");
    Assert.assertEquals(facets.get(1).getFacetValueHitCount(), 1);
    Assert.assertEquals(facets.get(1).getValue(), "2003");

    // check value retrieval and sorting
    Assert.assertEquals(3, topCollector.getTotalHits());
    ScoreDoc[] docs = topCollector.topDocs().scoreDocs;
    Assert.assertEquals(_reader.getFieldValue(request, docs[0].doc, "year"), "2005");
    Assert.assertEquals(_reader.getFieldValue(request, docs[1].doc, "year"), "2005");
    Assert.assertEquals(_reader.getFieldValue(request, docs[2].doc, "year"), "2003");
  }


  private Document createDoc(long year)
  {
    FieldType type = new FieldType();
    type.setIndexed(true);
    type.setOmitNorms(true);
    type.setIndexOptions(FieldInfo.IndexOptions.DOCS_ONLY);
    type.setTokenized(false);
    type.setDocValueType(FieldInfo.DocValuesType.NUMERIC);

    Document doc = new Document();
    doc.add(new NumericDocValuesField("year", year));
    doc.add(new StringField("year", String.valueOf(year), Field.Store.NO));
    return doc;
  }
}
