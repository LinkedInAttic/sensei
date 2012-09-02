package com.senseidb.ba.plugins;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import proj.zoie.api.ZoieIndexReader;

import com.browseengine.bobo.api.BoboIndexReader;
import com.browseengine.bobo.api.BrowseSelection;
import com.browseengine.bobo.api.FacetSpec;
import com.browseengine.bobo.facets.FacetCountCollectorSource;
import com.browseengine.bobo.facets.FacetHandler;
import com.browseengine.bobo.facets.filter.RandomAccessFilter;
import com.browseengine.bobo.facets.impl.SimpleFacetHandler;
import com.browseengine.bobo.sort.DocComparatorSource;
import com.senseidb.ba.IndexSegment;
import com.senseidb.ba.SegmentToZoieAdapter;
import com.senseidb.ba.facet.ZeusFacetHandler;
import com.senseidb.search.node.SenseiIndexReaderDecorator;

public class ZeusIndexReaderDecorator extends SenseiIndexReaderDecorator {
  final static String[] emptyString = new String[0];
  @Override
public BoboIndexReader decorate(ZoieIndexReader<BoboIndexReader> zoieReader) throws IOException {
  SegmentToZoieAdapter adapter = (SegmentToZoieAdapter<?>)zoieReader;
  final IndexSegment offlineSegment = adapter.getOfflineSegment();
  List<FacetHandler<?>> facetHandlers = new ArrayList(offlineSegment.getColumnTypes().size() + 1);
  
 
  for (String column : offlineSegment.getColumnTypes().keySet()) {
    facetHandlers.add(new ZeusFacetHandler(column, column, IndexSegment.class.getSimpleName()));
  }
  BoboIndexReader indexReader =  new BoboIndexReader(adapter,  facetHandlers, Collections.EMPTY_LIST, new BoboIndexReader.WorkArea(), false) {
    public void facetInit() throws IOException {
      putFacetData(IndexSegment.class.getSimpleName(), offlineSegment);
      super.facetInit();
    }
    {facetInit();}
  };
 
  return indexReader;
  }
 
}
