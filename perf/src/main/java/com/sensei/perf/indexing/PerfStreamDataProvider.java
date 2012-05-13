package com.sensei.perf.indexing;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

import org.apache.log4j.Logger;
import org.json.JSONObject;

import com.linkedin.zoie.api.DataConsumer.DataEvent;
import com.linkedin.zoie.impl.indexing.StreamDataProvider;

import com.sensei.indexing.api.DataSourceFilter;
import com.sensei.indexing.api.DataSourceFilterable;
import com.sensei.perf.indexing.PerfVersion.PerfVersionComparator;


public class PerfStreamDataProvider extends StreamDataProvider<JSONObject> implements DataSourceFilterable<String> {

	private static Logger logger = Logger.getLogger(PerfStreamDataProvider.class);
	
	private DataSourceFilter<String> _dataFilter;
	private final File _file;
	private RandomAccessFile _rad;
	private PerfVersion _offset;
	private PerfVersion _startingOffset;
	private final int _maxIter;
	
	
	public PerfStreamDataProvider(File file,PerfVersion startingOffset,int maxIter) {
		super(new PerfVersionComparator());
		_file = file;
		_startingOffset = startingOffset;
		_maxIter = maxIter;
	}

	@Override
	public DataEvent<JSONObject> next() {
		DataEvent<JSONObject> event = null;
		if (_rad!=null){
		  try{
			String line = _rad.readLine();
			if (line == null){
				if (_offset.iter >= _maxIter){
					return null;
				}
				else{
					_offset.iter++;
					_offset.version = 0L;
					_rad.seek(0L);
					line = _rad.readLine();
					if (line ==null) return null;
				}
			}
			JSONObject dataObj = _dataFilter.filter(line);
			String version = String.valueOf(_offset);
			long fp = _rad.getFilePointer();
			_offset.version = fp;
			event = new DataEvent<JSONObject>(dataObj,version);
		  }
		  catch(Exception ioe){
			logger.error(ioe.getMessage(),ioe);
		  }
		}
		return event;
	}

	@Override
	public void setStartingOffset(String version) {
		_startingOffset = PerfVersion.parse(version);
	}

	@Override
	public void reset() {
	  if (_rad!=null){
		  try {
			_offset = _startingOffset;
			_rad.seek(_offset.version);
		} catch (IOException e) {
			logger.error(e.getMessage(),e);
		}
	  }
	}

	@Override
	public void setFilter(DataSourceFilter<String> filter) {
		_dataFilter = filter;
	}

	@Override
	public void start() {
		super.start();
		
		try{
		  _offset = _startingOffset;

		  logger.info("data file: "+_file.getAbsolutePath()+", offset: "+_offset);
		  _rad = new RandomAccessFile(_file,"r");
		  _rad.seek(_offset.version);
		}
		catch(IOException ioe){
		  logger.error(ioe.getMessage(),ioe);
		}
	}

	@Override
	public void stop() {
		try{
		  try{
		    if (_rad!=null){
		    	_rad.close();
		    }
		  }
		  catch(IOException ioe){
			logger.error(ioe.getMessage(),ioe);
		  }
		  finally{
		    _rad = null;
		  }
		}
		finally{
		  super.stop();
		}
	}

}
