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


import java.io.IOException;
import java.util.List;

import com.senseidb.facet.FacetRequest;
import com.senseidb.facet.FacetSystem;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiReader;


/**
 * bobo browse index reader
 * 
 */
public class FacetMultiReader extends MultiReader
{
  private FacetSystem _bobo;

  public FacetMultiReader(FacetSystem bobo, IndexReader reader)  throws IOException
  {
    super(getSubReaders(bobo, reader));
    _bobo = bobo;
  }

  private static FacetAtomicReader[] getSubReaders(FacetSystem bobo, IndexReader reader) throws IOException
  {
    List<AtomicReaderContext> leaves = reader.leaves();
    FacetAtomicReader[] subReaders = new FacetAtomicReader[leaves.size()];
    for (int i = 0; i < leaves.size(); ++i)
    {
      subReaders[i] = new FacetAtomicReader(bobo, leaves.get(i).reader());
    }

    return subReaders;
  }

  public String[] getFieldValues(FacetRequest request, int docID, String field)
  {
    FacetAtomicReader atomicReader = (FacetAtomicReader)getSequentialSubReaders().get(readerIndex(docID));
    return atomicReader.getFieldValues(request, docID, field);
  }

  public Object[] getRawFieldValues(FacetRequest request, int docID, String field)
  {
    FacetAtomicReader atomicReader = (FacetAtomicReader)getSequentialSubReaders().get(readerIndex(docID));
    return atomicReader.getRawFieldValues(request, docID, field);
  }

  public String getFieldValue(FacetRequest request, int docID, String field)
  {
    String[] fields = getFieldValues(request, docID, field);
    if (fields!=null && fields.length > 0)
    {
      return fields[0];
    }
    else
    {
      return null;
    }
  }

  public Object getRawFieldValue(FacetRequest request, int docID, String field)
  {
    Object[] fields = getRawFieldValues(request, docID, field);
    if (fields!=null && fields.length > 0)
    {
      return fields[0];
    }
    else
    {
      return null;
    }
  }
}
