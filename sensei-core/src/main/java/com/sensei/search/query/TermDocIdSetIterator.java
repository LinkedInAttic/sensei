package com.sensei.search.query;

import java.io.IOException;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermDocs;
import org.apache.lucene.search.DocIdSetIterator;

public class TermDocIdSetIterator extends DocIdSetIterator {
	  private final TermDocs termDocs;
	  private int doc = -1;

	  private final int[] docs;         // buffered doc numbers
	  private final int[] freqs;         // buffered doc numbers
	  
	  private int pointer;
	  private int pointerMax;


	  /**
	   * Construct a <code>TermDocIdSetIterator</code>.
	   * @param term Term
	   * @param reader
	   *          IndexReader.
	   * @param buffer
	   *         Number of docs to buffered the read for
	   */
	  public TermDocIdSetIterator(Term term,IndexReader reader,int buffer) throws IOException{
		  termDocs = reader.termDocs(term);
		  docs = new int[buffer];
		  freqs = new int[buffer];
	  }
	  

	  /**
	   * Construct a <code>TermDocIdSetIterator</code>.
	   * @param term Term
	   * @param reader
	   *          IndexReader.
	   */
	  public TermDocIdSetIterator(Term term,IndexReader reader) throws IOException{
		    this(term,reader,32);
	  }

	  @Override
	  public int docID() { return doc; }

	  /**
	   * Advances to the next document matching the query. <br>
	   * The iterator over the matching documents is buffered using
	   * {@link TermDocs#read(int[],int[])}.
	   * 
	   * @return the document matching the query or NO_MORE_DOCS if there are no more documents.
	   */
	  @Override
	  public int nextDoc() throws IOException {
	    pointer++;
	    if (pointer >= pointerMax) {
	      pointerMax = termDocs.read(docs, freqs);    // refill buffer
	      if (pointerMax != 0) {
	        pointer = 0;
	      } else {
	        termDocs.close();                         // close stream
	        return doc = NO_MORE_DOCS;
	      }
	    } 
	    doc = docs[pointer];
	    return doc;
	  }
	  
	  /**
	   * Advances to the first match beyond the current whose document number is
	   * greater than or equal to a given target. <br>
	   * The implementation uses {@link TermDocs#skipTo(int)}.
	   * 
	   * @param target
	   *          The target document number.
	   * @return the matching document or NO_MORE_DOCS if none exist.
	   */
	  @Override
	  public int advance(int target) throws IOException {
	    // first scan in cache
	    for (pointer++; pointer < pointerMax; pointer++) {
	      if (docs[pointer] >= target) {
	        return doc = docs[pointer];
	      }
	    }

	    // not found in cache, seek underlying stream
	    boolean result = termDocs.skipTo(target);
	    if (result) {
	      pointerMax = 1;
	      pointer = 0;
	      docs[pointer] = doc = termDocs.doc();
	    } else {
	      doc = NO_MORE_DOCS;
	    }
	    return doc;
	  }

}
