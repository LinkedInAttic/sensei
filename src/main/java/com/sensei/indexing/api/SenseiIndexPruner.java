package com.sensei.indexing.api;

import java.io.IOException;

import com.browseengine.bobo.api.BoboIndexReader;
import com.sensei.search.req.SenseiRequest;

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
}
