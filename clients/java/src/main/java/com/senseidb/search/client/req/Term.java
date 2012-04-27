package com.senseidb.search.client.req;

import com.senseidb.search.client.req.query.Query;

/**
 * <p>
 * Matches documents that have fields that contain a term (<strong>not
 * analyzed</strong>). The term query maps to Sensei <code>TermQuery</code>. The
 * following matches documents where the user field contains the term
 * <code>kimchy</code>:
 * </p>
 * 
 */
public class Term extends Selection {
  private String value;
  private double boost;

  public Term(String value) {
    super();
    this.value = value;
  }

  public Term(String value, double boost) {
    super();
    this.value = value;
    this.boost = boost;
  }

  public Term() {
  }
}