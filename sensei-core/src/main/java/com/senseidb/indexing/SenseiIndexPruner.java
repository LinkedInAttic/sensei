package com.senseidb.indexing;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexReader;

import com.browseengine.bobo.api.BoboIndexReader;
import com.browseengine.bobo.api.BrowseSelection;
import com.browseengine.bobo.facets.FacetHandler;
import com.browseengine.bobo.facets.filter.EmptyFilter;
import com.browseengine.bobo.facets.filter.RandomAccessFilter;
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
	
	/**
	 * An implementation of index pruner which prunes indexes based on
	 * the age (the last time it was committed to disk) of an index segment. 
	 */
	public static class AgeBasedSelectionSenseiIndexPruner implements SenseiIndexPruner {
		
		long _maxAgeInDays;
		private static Logger _logger = Logger.getLogger(AgeBasedSelectionSenseiIndexPruner.class);

		/**
		 * Constructor
		 * @param days maximum age (in days) of an index beyond which it will be pruned.
		 */
		public AgeBasedSelectionSenseiIndexPruner(long days)
		{
			_maxAgeInDays = days;
		}
		
		 /**
		  */
		public void setMaxAge(long days)
		{
			_maxAgeInDays = days;
		}
		
		@Override
		public IndexReaderSelector getReaderSelector(final SenseiRequest req)
		{
			return new IndexReaderSelector() {
				@Override
				public  boolean isSelected(BoboIndexReader reader) throws IOException
				{

					long commitTime = IndexReader.lastModified(reader.directory());
					long diff = System.currentTimeMillis() - commitTime;
					long days = TimeUnit.DAYS.convert(diff, TimeUnit.MILLISECONDS);

					if (_logger.isDebugEnabled())
					{
						if (days > _maxAgeInDays)
						{
							_logger.debug("regjecting " + reader + " because it is "
								+ (days - _maxAgeInDays)
								+ " days older than max days allowed: " + _maxAgeInDays);
						}
					}
					
					return days < _maxAgeInDays;
				}
			};
		}
	}
}

