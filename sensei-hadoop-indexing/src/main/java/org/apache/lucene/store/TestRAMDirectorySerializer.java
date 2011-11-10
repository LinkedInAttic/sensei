package org.apache.lucene.store;

import java.io.IOException;
import java.io.StringReader;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Searcher;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.Version;

public class TestRAMDirectorySerializer {

	  public static void main(String[] args)
	  {
	    // Construct a RAMDirectory to hold the in-memory representation
	    // of the index.
	    RAMDirectory idx = new RAMDirectory();

	    try
	    {
	      // Make an writer to create the index
	      IndexWriter writer = new IndexWriter(idx, new StandardAnalyzer(Version.LUCENE_30), true, IndexWriter.MaxFieldLength.UNLIMITED);

	      // Add some Document objects containing quotes
	      writer.addDocument(createDocument("Steve Jobs",  "You should not sell apple stock."));
	      writer.addDocument(createDocument("Bill Gates", "I was the richest man in the world, you know it."));
	      writer.addDocument(createDocument("Mark Zuckerberg", "You guys are too old, I will be the richest, lol."));
	      writer.addDocument(createDocument("Larry Page", "I know what you have searched, hahahahahaha."));

	      // Optimize and close the writer to finish building the index
	      writer.optimize();
	      writer.close();

	      
	      byte[] bytes = RAMDirectorySerializer.toBytes(idx);
	      RAMDirectory idxNew = RAMDirectorySerializer.fromBytes(bytes);
	      
	      searchIndex(idx);
	      System.out.println("=========");
	      searchIndex(idxNew);
	      
	    }
	    catch (IOException ioe)
	    {
	      ioe.printStackTrace();
	    }
	    catch (ParseException pe)
	    {
	      pe.printStackTrace();
	    }
	  }

	  private static void searchIndex(RAMDirectory idx) throws CorruptIndexException, IOException, ParseException {
	      Searcher searcher = new IndexSearcher(idx);

	      search(searcher, "apple");
	      search(searcher, "richest");
	      search(searcher, "searched");

	      searcher.close();
	}

	  private static Document createDocument(String title, String content)
	  {
	    Document doc = new Document();

	    doc.add(new Field("title", title, Field.Store.YES, Field.Index.NO));
	    doc.add(new Field("content", new StringReader(content)));

	    return doc;
	  }

	  private static void search(Searcher searcher, String queryString)
	      throws ParseException, IOException
	  {

	    // Build a Query object
	    QueryParser parser = new QueryParser(Version.LUCENE_30, "content", new StandardAnalyzer(Version.LUCENE_30));
	    Query query = parser.parse(queryString);

	    int numHits = 100;
	    TopDocs topDocs = searcher.search(query, numHits);
	    ScoreDoc[] hits = topDocs.scoreDocs;
	    for (int i = 0; i < hits.length; i++) {
	      int docId = hits[i].doc;
	      float score = hits[i].score;
	      Document d = searcher.doc(docId);
	      // do something with current hit
	      System.out.println("docID:"+docId + "\tscore:"+score + "\t" + d.get("title"));
	    }
	    System.out.println();
	  }
}
