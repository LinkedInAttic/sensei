package com.senseidb.gateway.file;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.Comparator;

import org.apache.log4j.Logger;

import proj.zoie.api.DataConsumer.DataEvent;
import proj.zoie.impl.indexing.StreamDataProvider;

public abstract class LinedFileDataProvider<D> extends StreamDataProvider<D> {

	private static final Logger logger = Logger.getLogger(LinedFileDataProvider.class);
	
	private final File _file;
	private long _startingOffset;
	protected long _offset;
	
	private BufferedReader _reader;
	
	private static Charset UTF8 = Charset.forName("UTF-8");
	
	public LinedFileDataProvider(Comparator<String> versionComparator, File file,long startingOffset){
      super(versionComparator);
	  _file = file;
	  _reader = null;
	  _startingOffset = startingOffset;
	}
	
	protected abstract D convertLine(String line) throws IOException;
	
	@Override
	public DataEvent<D> next() {
		DataEvent<D> event = null;
		if (_reader!=null){
		  try{
			String line = _reader.readLine();
			if (line == null) return null;
			D dataObj = convertLine(line);
			String version = String.valueOf(_offset);
			_offset++;
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
    if (version != null)
      _startingOffset = Long.parseLong(version);
    else
      _startingOffset = 0;
	}

	@Override
	public void reset() {
	  if (_reader!=null){
		  try {
			_offset = _startingOffset;
			for (int i=0;i<_offset;++i){
        _reader.readLine();
      }
		} catch (IOException e) {
			logger.error(e.getMessage(),e);
		}
	  }
	}

	@Override
	public void start() {
		super.start();
		try{
		  _reader = new BufferedReader(new InputStreamReader(new FileInputStream(_file), UTF8),1024);
		  _offset = _startingOffset;
		  reset();
		}
		catch(IOException ioe){
		  logger.error(ioe.getMessage(),ioe);
		}
	}

	@Override
	public void stop() {
		try{
		  try{
		    if (_reader!=null){
		      _reader.close();
		    }
		  }
		  catch(IOException ioe){
			logger.error(ioe.getMessage(),ioe);
		  }
		  finally{
		    _reader = null;
		  }
		}
		finally{
		  super.stop();
		}
	}

	
	
}
