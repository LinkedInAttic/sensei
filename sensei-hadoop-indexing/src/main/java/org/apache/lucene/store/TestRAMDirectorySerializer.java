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
	      IndexWriter writer = new IndexWriter(idx, new StandardAnalyzer(Version.LUCENE_CURRENT), true, IndexWriter.MaxFieldLength.UNLIMITED);

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
	    QueryParser parser = new QueryParser(Version.LUCENE_CURRENT, "content", new StandardAnalyzer(Version.LUCENE_CURRENT));
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
