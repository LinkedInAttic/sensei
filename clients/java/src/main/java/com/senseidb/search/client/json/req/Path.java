package com.senseidb.search.client.json.req;

public class Path extends Selection {

  private String value;
  private boolean strict;
  private int depth;

  public Path(String value, boolean strict, int depth) {
    super();

    this.value = value;
    this.strict = strict;
    this.depth = depth;
  }

  public Path() {

  }

  public String getValue() {
    return value;
  }
  
}