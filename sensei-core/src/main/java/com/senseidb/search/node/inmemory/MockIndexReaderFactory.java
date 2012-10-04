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
package com.senseidb.search.node.inmemory;

import java.io.IOException;
import java.util.List;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.util.Version;

import proj.zoie.api.IndexReaderFactory;
import proj.zoie.api.ZoieIndexReader;

import com.browseengine.bobo.api.BoboIndexReader;

public class MockIndexReaderFactory<T> implements  IndexReaderFactory<ZoieIndexReader<BoboIndexReader> > {
  private Analyzer analyzer = new StandardAnalyzer(Version.LUCENE_35);
 
  private final List<ZoieIndexReader<BoboIndexReader>> readers;
  
  public MockIndexReaderFactory(List<ZoieIndexReader<BoboIndexReader>> readers) {
    this.readers = readers;
   
  }
  
  @Override
  public List<ZoieIndexReader<BoboIndexReader>> getIndexReaders() throws IOException {
    return readers;
    
  }
 
  @Override
  public Analyzer getAnalyzer() {
   
    return analyzer;
  }

  @Override
  public void returnIndexReaders(List<ZoieIndexReader<BoboIndexReader>> r) {
   
  }

  @Override
  public String getCurrentReaderVersion() {
    
    return null;
  }

}
