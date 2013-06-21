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
