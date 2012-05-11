package com.senseidb.search.client.req.relevance;

/**
 * "save as" part below is optional, if specified, this runtime generated model
 * will be saved with a name. After a runtime model is named, it will be
 * convenient to use next time, we can just specify the model name. Attn: Even
 * if we do not name a runtime model, the system will also automatically cache a
 * certain amount of anonymous runtime models, so there is no extra compilation
 * cost for second request with the same model function body and signature.
 * 
 */
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
