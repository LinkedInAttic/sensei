package com.senseidb.facet.handler;

import com.senseidb.facet.search.FacetAtomicReader;

import java.io.IOException;
import java.io.Serializable;
import java.util.Set;

/**
 * @author Dmytro Ivchenko
 */
public abstract class LoadableFacetHandler<D> extends FacetHandler {
  public static class FacetDataNone implements Serializable {
    private static final long serialVersionUID = 1L;
    public static FacetDataNone instance = new FacetDataNone();

    private FacetDataNone() {
    }
  }

  public LoadableFacetHandler(String name, Set<String> dependsOn) {
    super(name, dependsOn);
  }

  public LoadableFacetHandler(String name) {
    this(name, null);
  }

  /**
   * Load information from an index reader, initialized by {@link com.senseidb.facet.search.FacetAtomicReader}
   *
   * @param reader reader
   * @throws java.io.IOException
   */
  abstract public D load(FacetAtomicReader reader) throws IOException;

  @SuppressWarnings("unchecked")
  public D getFacetData(FacetAtomicReader reader) {
    return (D) reader.getFacetData(_name);
  }

  public D load(FacetAtomicReader reader, FacetAtomicReader.WorkArea workArea) throws IOException {
    return load(reader);
  }

}
