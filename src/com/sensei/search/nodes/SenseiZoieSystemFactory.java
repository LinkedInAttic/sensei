package com.sensei.search.nodes;

import java.io.File;
import java.io.FileNotFoundException;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.search.Similarity;

import com.browseengine.bobo.api.BoboIndexReader;

import proj.zoie.api.indexing.IndexReaderDecorator;
import proj.zoie.api.indexing.ZoieIndexableInterpreter;
import proj.zoie.impl.indexing.ZoieSystem;

public class SenseiZoieSystemFactory<V>
{
  protected File _idxDir;
  protected ZoieIndexableInterpreter<V> _interpreter;
  protected IndexReaderDecorator<BoboIndexReader> _indexReaderDecorator;
  protected Analyzer _analyzer;
  protected Similarity _similarity;
  protected int _batchSize;
  protected long _batchDelay;
  protected boolean _rtIndexing;
  
  public SenseiZoieSystemFactory(File idxDir, ZoieIndexableInterpreter<V> interpreter, IndexReaderDecorator<BoboIndexReader> indexReaderDecorator,
                                 Analyzer analyzer, Similarity similarity, int batchSize, long batchDelay, boolean rtIndexing)
  {
    _idxDir = idxDir;
    _interpreter = interpreter;
    _indexReaderDecorator = indexReaderDecorator;
    _analyzer = analyzer;
    _similarity = similarity;
    _batchSize = batchSize;
    _batchDelay = batchDelay;
    _rtIndexing = rtIndexing;
  }
  
  public ZoieSystem<BoboIndexReader,V> getZoieSystem(int partitionId) throws FileNotFoundException
  {
    File partDir = getPath(partitionId);
    if(!partDir.exists())
    {
      throw new FileNotFoundException("partition=" + partitionId + " does not exist");
    }
    return new ZoieSystem<BoboIndexReader,V>(getPath(partitionId), _interpreter, _indexReaderDecorator, _analyzer, _similarity, _batchSize, _batchDelay, _rtIndexing);
  }
  
  // TODO: change to getDirectoryManager
  protected File getPath(int partitionId)
  {
    return new File(_idxDir, "shard"+partitionId);
  }
}
