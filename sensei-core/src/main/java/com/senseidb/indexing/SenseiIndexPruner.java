package com.senseidb.indexing;

import java.io.IOException;

import com.linkedin.bobo.api.BoboIndexReader;
import com.linkedin.bobo.api.BrowseSelection;
import com.linkedin.bobo.facets.FacetHandler;
import com.linkedin.bobo.facets.filter.EmptyFilter;
import com.linkedin.bobo.facets.filter.RandomAccessFilter;
import com.senseidb.search.req.SenseiRequest;

public interface SenseiIndexPruner {
	
	IndexReaderSelector getReaderSelector(SenseiRequest req);
 
	public interface IndexReaderSelector{
		boolean isSelected(BoboIndexReader reader) throws IOException;
	}
	
	public static class DefaultSenseiIndexPruner implements SenseiIndexPruner{

		@Override
		public IndexReaderSelector getReaderSelector(SenseiRequest req) {
			return new IndexReaderSelector(){

				@Override
				public boolean isSelected(BoboIndexReader reader)
						throws IOException {
					return true;
				}
				
			};
		}
		
	}
	
	public static class BoboSelectionSenseiIndexPruner implements SenseiIndexPruner{

		@Override
		public IndexReaderSelector getReaderSelector(final SenseiRequest req) {
			return new IndexReaderSelector(){

				@Override
				public boolean isSelected(BoboIndexReader reader)
						throws IOException {
					BrowseSelection[] selections = req.getSelections();
					boolean valid = true;
					if (selections!=null){
						for (BrowseSelection sel : selections){
							String name = sel.getFieldName();
							FacetHandler<?> handler = reader.getFacetHandler(name);
							if (handler!=null){
								RandomAccessFilter filter = handler.buildFilter(sel);
								if (EmptyFilter.getInstance() == filter){
									valid = false;
									break;
								}
							}
						}
					}
					return valid;
				}
				
			};
		}
	}
}
