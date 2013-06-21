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
package com.senseidb.search.node;

import java.io.IOException;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

import proj.zoie.api.DataProvider;
import proj.zoie.api.IndexReaderFactory;
import proj.zoie.api.Zoie;
import proj.zoie.api.ZoieException;
import proj.zoie.api.ZoieIndexReader;
import proj.zoie.impl.indexing.ZoieSystem;

import com.browseengine.bobo.api.BoboIndexReader;
import com.browseengine.bobo.facets.FacetHandler;
import com.browseengine.bobo.facets.RuntimeFacetHandlerFactory;
import com.google.common.base.Preconditions;
import com.senseidb.indexing.SenseiIndexPruner;
import com.senseidb.indexing.SenseiIndexPruner.DefaultSenseiIndexPruner;
import com.senseidb.jmx.JmxUtil;
import com.senseidb.search.plugin.PluggableSearchEngineManager;
import com.senseidb.search.req.SenseiSystemInfo;


public class SenseiCore{
  private static final Logger logger = Logger.getLogger(SenseiServer.class);

  private SenseiZoieFactory<?> _zoieFactory;
  private SenseiIndexingManager _indexManager;
  private SenseiQueryBuilderFactory _queryBuilderFactory;
  private final HashSet<Zoie<BoboIndexReader,?>> zoieSystems = new HashSet<Zoie<BoboIndexReader,?>>();

  private final int[] _partitions;
  private final int _id;
  private final Map<Integer,Zoie<BoboIndexReader,?>> _readerFactoryMap;
  private SenseiSystemInfo _senseiSystemInfo;
  private volatile boolean _started;
  private SenseiIndexPruner _pruner;

  private PluggableSearchEngineManager pluggableSearchEngineManager;

  private final SenseiIndexReaderDecorator decorator;
    
  public SenseiCore(int id,int[] partitions,
            SenseiZoieFactory<?> zoieSystemFactory,
            SenseiIndexingManager indexManager,
            SenseiQueryBuilderFactory queryBuilderFactory,
            SenseiIndexReaderDecorator decorator){

    _zoieFactory = zoieSystemFactory;
    _indexManager = indexManager;
    _queryBuilderFactory = queryBuilderFactory;
    _partitions = partitions;
    _id = id;
    this.decorator = decorator;
    
    _readerFactoryMap = new HashMap<Integer,Zoie<BoboIndexReader,?>>();
    _started = false;
    _pruner = null;
  }
  
  public void setIndexPruner(SenseiIndexPruner pruner){
	_pruner = pruner;
  }
  
  public SenseiIndexPruner getIndexPruner(){
	  return _pruner == null ? new DefaultSenseiIndexPruner() : _pruner;
  }
  
  public int getNodeId(){
    return _id;
  }
  
  public int[] getPartitions(){
    return _partitions;
  }

  public SenseiSystemInfo getSystemInfo()
  {
    if (_senseiSystemInfo == null)
      _senseiSystemInfo = new SenseiSystemInfo();

    //if (_senseiSystemInfo.getClusterInfo() == null)
    //{
      //List<Integer> partitionList = new ArrayList<Integer>(_partitions.length);

      //for (int i=0; i<_partitions.length; ++i)
      //{
        //partitionList.add(_partitions[i]);
      //}

      //Map<Integer, List<Integer>> clusterInfo = new HashMap<Integer, List<Integer>>();
      //clusterInfo.put(_id, partitionList);
      //_senseiSystemInfo.setClusterInfo(clusterInfo);
    //}

    if (_senseiSystemInfo.getFacetInfos() == null)
    {
      Set<SenseiSystemInfo.SenseiFacetInfo> facetInfos = new HashSet<SenseiSystemInfo.SenseiFacetInfo>();
      if (_zoieFactory.getDecorator() != null && _zoieFactory.getDecorator().getFacetHandlerList() != null)
      {
        for (FacetHandler<?> facetHandler : _zoieFactory.getDecorator().getFacetHandlerList())
        {
          facetInfos.add(new SenseiSystemInfo.SenseiFacetInfo(facetHandler.getName()));
        }
      }

      if (_zoieFactory.getDecorator() != null && _zoieFactory.getDecorator().getFacetHandlerFactories() != null)
      {
        for (RuntimeFacetHandlerFactory<?,?> runtimeFacetHandlerFactory : _zoieFactory.getDecorator().getFacetHandlerFactories())
        {
          SenseiSystemInfo.SenseiFacetInfo facetInfo = new SenseiSystemInfo.SenseiFacetInfo(runtimeFacetHandlerFactory.getName());
          facetInfo.setRunTime(true);
          facetInfos.add(facetInfo);
        }
      }
      _senseiSystemInfo.setFacetInfos(facetInfos);
    }

    Date lastModified = new Date(0L);
    String version = null;
    for(Zoie<BoboIndexReader,?> zoieSystem : zoieSystems)
    {
      if (version == null || _zoieFactory.getVersionComparator().compare(version, zoieSystem.getVersion()) < 0)
        version = zoieSystem.getVersion();
      
    }
/*
    for (ObjectName name : _registeredMBeans) {
      try
      {
        Date lastModifiedB = (Date)mbeanServer.getAttribute(name, "LastDiskIndexModifiedTime");
        if (lastModified.compareTo(lastModifiedB) < 0)
          lastModified = lastModifiedB;
      }
      catch (Exception e)
      {
        // Simplely ignore.
      }
    }
    */
    
    // TODO: fix this after zoie/hourglass jmx is done: http://linkedin.jira.com/browse/ZOIE-81
    _senseiSystemInfo.setLastModified(lastModified.getTime());
    if (version != null)
      _senseiSystemInfo.setVersion(version);

    return _senseiSystemInfo;
  }

  public void setSystemInfo(SenseiSystemInfo senseiSystemInfo)
  {
    _senseiSystemInfo = senseiSystemInfo;
  }
  
  public void start() throws Exception{
    if (_started) return;
      for (int part : _partitions){
        
        Zoie<BoboIndexReader,?> zoieSystem = _zoieFactory.getZoieInstance(_id,part);
        
        // register ZoieSystemAdminMBean

        String[] mbeannames = zoieSystem.getStandardMBeanNames();
        for(String name : mbeannames)
        {
          JmxUtil.registerMBean(zoieSystem.getStandardMBean(name), "zoie-name", name + "-" + _id+"-"+part);  
        }
              
        if(!zoieSystems.contains(zoieSystem))
        {
          zoieSystem.start();
          zoieSystems.add(zoieSystem);
        }

        _readerFactoryMap.put(part, zoieSystem);
      }
      try{
        pluggableSearchEngineManager.start(this);
        logger.info("initializing index manager...");
        if (_indexManager!=null){
          _indexManager.initialize(_readerFactoryMap);
        }
        logger.info("starting index manager...");
        if (_indexManager!=null){
          _indexManager.start();
        }
      
        logger.info("index manager started...");
      }
      catch(Exception e){
        logger.error("Unable to start indexing manager, indexing not started...",e);
      }
      _started = true;
  }

  public void shutdown() {
    if (!_started) return;
    logger.info("unregistering mbeans...");
    // shutdown the index manager

    logger.info("Closing the pluggable index manager...");
    pluggableSearchEngineManager.close();

    logger.info("shutting down index manager...");
    if (_indexManager != null) {
      _indexManager.shutdown();
    }
    logger.info("index manager shutdown...");

    // shutdown the zoieSystems
    for (Zoie<BoboIndexReader, ?> zoieSystem : zoieSystems) {
      zoieSystem.shutdown();
    }
    zoieSystems.clear();
    _started = false;
  }

  public DataProvider getDataProvider()
  {
    return _indexManager.getDataProvider();
  }
  
  public IndexReaderFactory<ZoieIndexReader<BoboIndexReader>> getIndexReaderFactory(int partition)
  {
    IndexReaderFactory<ZoieIndexReader<BoboIndexReader>> readerFactory = _readerFactoryMap.get(partition);
    if (readerFactory == null)
    {
      logger.error("IndexReaderFactory not found for partition: " + partition + ". I'm serving partition " + _readerFactoryMap.keySet() + " only.  Please check the routing strategy.");
    }
    return readerFactory;
  }

  public SenseiQueryBuilderFactory getQueryBuilderFactory()
  {
    return _queryBuilderFactory;
  }
  
  public Collection<Zoie<BoboIndexReader, ?>> getZoieSystems()
  {
    return zoieSystems;
  }

  public int getNumZoieSystems()
  {
    return zoieSystems.size();
  }
  
  // Export snapshot from zoie to list of channels.
  public void exportSnapshot(List<WritableByteChannel> channels) throws IOException
  {
    Preconditions.checkNotNull(channels);
    Preconditions.checkArgument(channels.size() > 0);
    Preconditions.checkArgument(channels.size() >= zoieSystems.size());
    
    int i = 0;
    
    // TODO: allow run in parallel
    
    for (Zoie<BoboIndexReader,?> zoieSystem : zoieSystems)
    {
      if (zoieSystem instanceof ZoieSystem)
      {
        try
        {
          ((ZoieSystem<BoboIndexReader,?>)zoieSystem).exportSnapshot(channels.get(i++));
        }
        catch (IOException e)
        {
          logger.error("exportSnapshot " + i, e);
          throw new IOException(e);
        }
      }
    }
  }

  // Import snapshot to zoie from list of channels.
  public void importSnapshot(List<ReadableByteChannel> channels) throws IOException
  {
    Preconditions.checkNotNull(channels);
    Preconditions.checkArgument(channels.size() > 0);
    Preconditions.checkArgument(channels.size() >= zoieSystems.size());
    
    int i = 0;
    
    // TODO: allow run in parallel
    
    for (Zoie<BoboIndexReader,?> zoieSystem : zoieSystems)
    {
      if (zoieSystem instanceof ZoieSystem)
      {
        try
        {
          ((ZoieSystem<BoboIndexReader,?>)zoieSystem).importSnapshot(channels.get(i++));
        }
        catch (IOException e)
        {
          logger.error("exportSnapshot " + i, e);
          throw new IOException(e);
        }
      }
    }
  }

  public void optimize()
  {
    for (Zoie<BoboIndexReader,?> zoieSystem : zoieSystems)
    {
      if (zoieSystem instanceof ZoieSystem)
      {
        ((ZoieSystem<BoboIndexReader,?>)zoieSystem).optimize();
      }
    }
  }

  public void syncWithVersion(long timeToWait, String version) throws ZoieException
  {
    _indexManager.syncWithVersion(timeToWait, version);
  }

  public void setPluggableSearchEngineManager(PluggableSearchEngineManager pluggableSearchEngineManager) {
    this.pluggableSearchEngineManager = pluggableSearchEngineManager;
  }

  public PluggableSearchEngineManager getPluggableSearchEngineManager() {
    return pluggableSearchEngineManager;
  }

  public SenseiIndexReaderDecorator getDecorator() {
    return decorator;
  }
  
}
