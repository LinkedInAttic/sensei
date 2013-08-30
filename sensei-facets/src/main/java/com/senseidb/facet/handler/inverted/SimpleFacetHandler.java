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

package com.senseidb.facet.handler.inverted;


import com.senseidb.facet.FacetSelection;
import com.senseidb.facet.FacetSpec;
import com.senseidb.facet.termlist.TermListFactory;
import com.senseidb.facet.filter.EmptyFilter;
import com.senseidb.facet.filter.NotFilter;
import com.senseidb.facet.handler.FacetCountCollector;
import com.senseidb.facet.handler.FacetCountCollectorSource;
import com.senseidb.facet.handler.FacetHandler;
import com.senseidb.facet.search.FacetAtomicReader;
import org.apache.lucene.search.FieldComparatorSource;
import org.apache.lucene.search.Filter;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Set;

public class SimpleFacetHandler extends FacetHandler<FacetDataCache>
{
	protected TermListFactory _termListFactory;
	protected final String _indexFieldName;
  protected final int _invertedIndexPenalty;
	
	public SimpleFacetHandler(String name,
                            String indexFieldName,
                            TermListFactory termListFactory,
                            Set<String> dependsOn,
                            int invertedIndexPenalty)
	{
	   super(name,dependsOn);
	   _indexFieldName=indexFieldName;
	   _termListFactory=termListFactory;
    _invertedIndexPenalty = invertedIndexPenalty;
	}

  public SimpleFacetHandler(String name,
                            String indexFieldName,
                            TermListFactory termListFactory,
                            Set<String> dependsOn)
  {
    this(name, indexFieldName, termListFactory, dependsOn, AdaptiveFacetFilter.DEFAULT_INVERTED_INDEX_PENALTY);
  }


  public SimpleFacetHandler(String name,
                            TermListFactory termListFactory,
                            Set<String> dependsOn,
                            int invertedIndexPenalty)
	{
	   this(name,name,termListFactory,dependsOn, invertedIndexPenalty);
	}
	
	public SimpleFacetHandler(String name, String indexFieldName, TermListFactory termListFactory, int invertedIndexPenalty)
	{
	   this(name,indexFieldName,termListFactory,null, invertedIndexPenalty);
	}
	
	public SimpleFacetHandler(String name, TermListFactory termListFactory, int invertedIndexPenalty)
    {
        this(name,name,termListFactory, invertedIndexPenalty);
    }
	
	public SimpleFacetHandler(String name, int invertedIndexPenalty)
    {
        this(name,name,null, invertedIndexPenalty);
    }
	
	public SimpleFacetHandler(String name, String indexFieldName, int invertedIndexPenalty)
	{
		this(name,indexFieldName,null, invertedIndexPenalty);
	}

  public SimpleFacetHandler(String name,
                            TermListFactory termListFactory,
                            Set<String> dependsOn)
  {
    this(name,name,termListFactory,dependsOn, AdaptiveFacetFilter.DEFAULT_INVERTED_INDEX_PENALTY);
  }

  public SimpleFacetHandler(String name, String indexFieldName, TermListFactory termListFactory)
  {
    this(name, indexFieldName, termListFactory,null, AdaptiveFacetFilter.DEFAULT_INVERTED_INDEX_PENALTY);
  }

  public SimpleFacetHandler(String name, TermListFactory termListFactory)
  {
    this(name, name, termListFactory, AdaptiveFacetFilter.DEFAULT_INVERTED_INDEX_PENALTY);
  }

  public SimpleFacetHandler(String name)
  {
    this(name, name, null, AdaptiveFacetFilter.DEFAULT_INVERTED_INDEX_PENALTY);
  }

  public SimpleFacetHandler(String name, String indexFieldName)
  {
    this(name,indexFieldName, null, AdaptiveFacetFilter.DEFAULT_INVERTED_INDEX_PENALTY);
  }


  @Override
	public int getNumItems(FacetAtomicReader reader, int id) {
		FacetDataCache data = getFacetData(reader);
		if (data==null) return 0;
		return data.getNumItems(id);
	}

  @Override
  public FieldComparatorSource getFieldComparatorSource() {
    return new FacetDataCache.FacetFieldComparatorSource(this);
  }

	@Override
	public String[] getFieldValues(FacetAtomicReader reader,int id) {
		FacetDataCache dataCache = getFacetData(reader);
		if (dataCache!=null){
		  return new String[]{dataCache.valArray.get(dataCache.orderArray.get(id))};
		}
		return new String[0];
	}

	@Override
	public Object[] getRawFieldValues(FacetAtomicReader reader,int id){
		FacetDataCache dataCache = getFacetData(reader);
		if (dataCache!=null){
		  return new Object[]{dataCache.valArray.getRawValue(dataCache.orderArray.get(id))};
		}
		return new String[0];
	}
	
  @Override
  public Filter buildFilter(String value) throws IOException
  {
    FacetFilter f = new FacetFilter(this, value);
    AdaptiveFacetFilter af = new AdaptiveFacetFilter(new AdaptiveFacetFilter.FacetDataCacheBuilder(){

		@Override
		public FacetDataCache build(FacetAtomicReader reader) {
			return  getFacetData(reader);
		}

		@Override
		public String getName() {
			return SimpleFacetHandler.this.getName();
		}

		@Override
    public String getIndexFieldName() {
      return _indexFieldName;
    }

    }, f, Collections.singletonList(value), false, _invertedIndexPenalty);
    return af;
  }

  @Override
  public Filter buildAndFilter(List<String> vals) throws IOException
  {
    if (vals.size() > 1)
    {
      return EmptyFilter.getInstance();
    }
    else
    {
      return buildFilter(vals.get(0));
    }
  }

  @Override
  public Filter buildOrFilter(List<String> vals, boolean isNot) throws IOException
  {
    Filter filter;
    
    if(vals.size() > 1)
    {
      Filter f = new FacetOrFilter(this,vals,false);
      filter = new AdaptiveFacetFilter(new AdaptiveFacetFilter.FacetDataCacheBuilder(){

  		@Override
  		public FacetDataCache build(FacetAtomicReader reader) {
  			return  getFacetData(reader);
  		}

      @Override
      public String getName() {
        return SimpleFacetHandler.this.getName();
      }

      @Override
      public String getIndexFieldName() {
        return _indexFieldName;
      }

      }, f, vals, isNot, _invertedIndexPenalty);
    }
    else if(vals.size() == 1)
    {
      filter = buildFilter(vals.get(0));
    }
    else
    {
      filter = EmptyFilter.getInstance();
    }
    
    if (isNot)
    {
      filter = new NotFilter(filter);
    }
    
    return filter;
  }

  @Override
	public FacetCountCollectorSource getFacetCountCollectorSource(final FacetSelection sel,final FacetSpec ospec) {
    return new FacetCountCollectorSource(){
      @Override
      public FacetCountCollector getFacetCountCollector(
          FacetAtomicReader reader) {
        FacetDataCache dataCache = SimpleFacetHandler.this.getFacetData(reader);
        return new SimpleFacetCountCollector(_name,dataCache, sel, ospec);
      }
    };
	}

	@Override
	public FacetDataCache load(FacetAtomicReader reader) throws IOException {
		FacetDataCache dataCache = new FacetDataCache();
		dataCache.load(_indexFieldName, reader, _termListFactory);
		return dataCache;
	}
	
	public static final class SimpleFacetCountCollector extends DefaultFacetCountCollector
	{
		public SimpleFacetCountCollector(String name,FacetDataCache dataCache,FacetSelection sel,FacetSpec ospec)
		{
		    super(name,dataCache,sel,ospec);
		}
		
		public final void collect(int docid) {
      int index = _array.get(docid);
      _count.add(index, _count.get(index) + 1);
		}
	}
}
