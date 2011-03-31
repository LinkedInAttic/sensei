package com.sensei.dataprovider.file;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.StringTokenizer;

import org.apache.log4j.Logger;
import org.json.JSONObject;

import proj.zoie.api.DataConsumer.DataEvent;
import proj.zoie.api.DefaultZoieVersion;
import proj.zoie.impl.indexing.StreamDataProvider;

public abstract class LinedFileDataProvider<D> extends StreamDataProvider<D, DefaultZoieVersion> {

	private static final Logger logger = Logger.getLogger(LinedFileDataProvider.class);
	
	private final File _file;
	private long _offset;
	
	private RandomAccessFile _rad;
	
	public LinedFileDataProvider(File file,long startingOffset){
	  _file = file;
	  _rad = null;
	  _offset = startingOffset;
	}
	
	protected abstract D convertLine(String line) throws IOException;
	
	@Override
	public DataEvent<D, DefaultZoieVersion> next() {
		DataEvent<D,DefaultZoieVersion> event = null;
		if (_rad!=null){
		  try{
			String line = _rad.readLine();
			D dataObj = convertLine(line);
			DefaultZoieVersion version = new DefaultZoieVersion();
			version.setVersionId(_offset);
			_offset = _rad.getFilePointer();
			event = new DataEvent<D,DefaultZoieVersion>(dataObj,version);
		  }
		  catch(IOException ioe){
			logger.error(ioe.getMessage(),ioe);
		  }
		}
		return event;
	}
	
	

	@Override
	public void setStartingOffset(DefaultZoieVersion version) {
		_offset = version.getVersionId();
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
