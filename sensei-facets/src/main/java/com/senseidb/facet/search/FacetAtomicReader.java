/**
 * Bobo Browse Engine - High performance faceted/parametric search implementation 
 * that handles various types of semi-structured data.  Written in Java.
 *
 * Copyright (C) 2005-2006  John Wang
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301  USA
 *
 * To contact the project administrators for the bobo-browse project, 
 * please go to https://sourceforge.net/projects/bobo-browse/, or 
 * send mail to owner@browseengine.com.
 */

package com.senseidb.facet.search;


import com.senseidb.facet.FacetRequest;
import com.senseidb.facet.FacetSystem;
import com.senseidb.facet.handler.FacetHandler;
import com.senseidb.facet.handler.LoadableFacetHandler;
import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.FilterAtomicReader;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;


/**
 * bobo browse index reader
 */
public class FacetAtomicReader extends FilterAtomicReader {

  private FacetSystem _bobo;
  private final WorkArea _workArea;
  private final Map<String, Object> _facetDataMap = new HashMap<String, Object>();

  public FacetAtomicReader(FacetSystem bobo, AtomicReader reader) throws IOException {
    super(reader);
    _bobo = bobo;
    _workArea = new WorkArea();

    for (Map.Entry<String, FacetHandler> entry : bobo.getFacetHandlerMap().entrySet()) {
      if (entry.getValue() instanceof LoadableFacetHandler<?>) {
        Object data = ((LoadableFacetHandler<?>)entry.getValue()).load(this, _workArea);
        _facetDataMap.put(entry.getKey(), data);
      }
    }

    for (Map.Entry<String, FacetHandler> entry : bobo.getFacetHandlerMap().entrySet()) {
      processDependencies(entry.getValue());
    }
  }

  private void processDependencies(FacetHandler facetHandler) throws IOException {
    for (String name : facetHandler.getDependsOn()) {
      FacetHandler dep = _bobo.getFacetHandlerMap().get(name);
      if (dep == null)
        throw new IOException("Dependent facet handler not found," + name);
      processDependencies(dep);
    }
  }

  public Object getFacetData(String name) {
    return _facetDataMap.get(name);
  }

  public Object putFacetData(String name, Object data) {
    return _facetDataMap.put(name, data);
  }

  public String[] getFieldValues(FacetRequest request, int docID, String field)
  {
    FacetHandler handler = request.getAllFacetHandlerMap().get(field);
    if (null == handler)
      return null;

    return handler.getFieldValues(this, docID);
  }

  public Object[] getRawFieldValues(FacetRequest request, int docID, String field)
  {
    FacetHandler handler = request.getAllFacetHandlerMap().get(field);
    if (null == handler)
      return null;

    return handler.getRawFieldValues(this, docID);
  }

  /**
   * Work area for loading
   */
  public static class WorkArea {
    HashMap<Class<?>, Object> map = new HashMap<Class<?>, Object>();

    @SuppressWarnings("unchecked")
    public <T> T get(Class<T> cls) {
      T obj = (T) map.get(cls);
      return obj;
    }

    public void put(Object obj) {
      map.put(obj.getClass(), obj);
    }


    public void clear() {
      map.clear();
    }

    @Override
    public String toString() {
      return map.toString();
    }
  }
}
