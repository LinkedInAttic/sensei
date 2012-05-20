package com.senseidb.search.node;

import java.io.File;

import org.apache.log4j.Logger;

import com.linkedin.zoie.api.indexing.ZoieIndexableInterpreter;
import com.linkedin.zoie.api.DirectoryManager.DIRECTORY_MODE;
import com.linkedin.zoie.api.IndexCopier;
import com.linkedin.zoie.api.Zoie;
import com.linkedin.zoie.impl.indexing.ZoieConfig;
import com.linkedin.zoie.pair.impl.Pair;

import com.linkedin.bobo.api.BoboIndexReader;

public class SenseiPairFactory<T> extends SenseiZoieFactory<T>
{
  private static Logger log = Logger.getLogger(SenseiPairFactory.class);

  private final static String ZOIEONE_DIR = "zoieone";

  private File              _zoieOneRoot;
  private IndexCopier       _indexCopier;
  private SenseiZoieFactory _zoieTwoFactory;

  public SenseiPairFactory(File                        idxDir,
                           DIRECTORY_MODE              dirMode,
                           IndexCopier                 indexCopier,
                           ZoieIndexableInterpreter<T> interpreter,
                           SenseiIndexReaderDecorator  indexReaderDecorator,
                           ZoieConfig                  zoieConfig,
                           SenseiZoieFactory<T>        zoieTwoFactory)
  {
    super(idxDir, dirMode,interpreter, indexReaderDecorator, zoieConfig);

    _zoieOneRoot      = new File(idxDir, ZOIEONE_DIR);
    _indexCopier      = indexCopier;
    _zoieTwoFactory   = zoieTwoFactory;
  }

  @Override
  public Pair<BoboIndexReader, T> getZoieInstance(int nodeId, int partitionId)
  {
    File zoieOneDir = getPath(nodeId, partitionId);
    if(!zoieOneDir.exists())
    {
      zoieOneDir.mkdirs();
      log.info("zoieOne: nodeId="+nodeId+", partition=" + partitionId +
               " does not exist, directory created.");
    }
    Zoie<BoboIndexReader, T> zoieTwo = _zoieTwoFactory.getZoieInstance(nodeId, partitionId);
    Pair<BoboIndexReader, T> zoie = new Pair<BoboIndexReader, T>(zoieOneDir,_dirMode,
                                                                 _indexCopier,
                                                                 _interpreter,
                                                                 _indexReaderDecorator,
                                                                 _zoieConfig,
                                                                 zoieTwo);
    return zoie;
  }

  public File getPath(int nodeId, int partitionId)
  {
    return getPath(_zoieOneRoot, nodeId, partitionId);
  }
}
