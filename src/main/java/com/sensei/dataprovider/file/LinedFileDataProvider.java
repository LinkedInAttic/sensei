package com.sensei.dataprovider.file;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Comparator;

import org.apache.log4j.Logger;

import proj.zoie.api.DataConsumer.DataEvent;
import proj.zoie.impl.indexing.StreamDataProvider;

public abstract class LinedFileDataProvider<D> extends StreamDataProvider<D> {

	private static final Logger logger = Logger.getLogger(LinedFileDataProvider.class);
	
	private final File _file;
	private long _offset;
	
	private RandomAccessFile _rad;
	
	public LinedFileDataProvider(Comparator<String> versionComparator, File file,long startingOffset){
    super(versionComparator);
	  _file = file;
	  _rad = null;
	  _offset = startingOffset;
	}
	
	protected abstract D convertLine(String line) throws IOException;
	
	@Override
	public DataEvent<D> next() {
		DataEvent<D> event = null;
		if (_rad!=null){
		  try{
			String line = _rad.readLine();
			if (line == null) return null;
			D dataObj = convertLine(line);
			String version = String.valueOf(_offset);
			_offset = _rad.getFilePointer();
			event = new DataEvent<D>(dataObj,version);
		  }
		  catch(IOException ioe){
			logger.error(ioe.getMessage(),ioe);
		  }
		}
		return event;
	}
	
	

	@Override
	public void setStartingOffset(String version) {
		_offset = Long.parseLong(version);
	}

	@Override
	public void reset() {
	  if (_rad!=null){
		  try {
			_offset = 0;
			_rad.seek(_offset);
		} catch (IOException e) {
			logger.error(e.getMessage(),e);
		}
	  }
	}

	@Override
	public void start() {
		super.start();
		try{
		  _rad = new RandomAccessFile(_file,"r");
		  _rad.seek(_offset);
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
