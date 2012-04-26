package com.senseidb.search.client.json.req.relevance;

public class SaveAs {
  private String name;
  private boolean overwrite;
  public SaveAs(String name, boolean overwrite) {
    super();
    this.name = name;
    this.overwrite = overwrite;
  }
  public String getName() {
    return name;
  }
  public boolean isOverwrite() {
    return overwrite;
  }
  
}
