package com.sensei.indexing.api;

import java.lang.management.ManagementFactory;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import javax.management.StandardMBean;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.log4j.Logger;
import org.json.JSONObject;
import org.springframework.context.ApplicationContext;

import proj.zoie.api.DataConsumer;
import proj.zoie.api.DataConsumer.DataEvent;
import proj.zoie.api.DataProvider;
import proj.zoie.api.Zoie;
import proj.zoie.api.ZoieException;
import proj.zoie.impl.indexing.StreamDataProvider;
import proj.zoie.mbean.DataProviderAdmin;
import proj.zoie.mbean.DataProviderAdminMBean;

import com.browseengine.bobo.api.BoboIndexReader;
import com.sensei.conf.SenseiSchema;
import com.sensei.indexing.api.gateway.SenseiGateway;
import com.sensei.indexing.api.gateway.SenseiGatewayRegistry;
import com.sensei.search.jmx.JmxUtil;
import com.sensei.search.nodes.SenseiIndexingManager;

public class DefaultStreamingIndexingManager implements SenseiIndexingManager<JSONObject> {

	private static final Logger logger = Logger.getLogger(DefaultStreamingIndexingManager.class);

	private static final String CONFIG_PREFIX = "sensei.index.manager.default";
	
	private static final String MAX_PARTITION_ID = "maxpartition.id";
	
	private static final String PROVIDER_TYPE = "type";
	
	private static final String EVTS_PER_MIN = "eventsPerMin";
	
	private static final String BATCH_SIZE = "batchSize";
	
	private static final String SHARDING_STRATEGY = "shardingStrategy";
	
	
	private StreamDataProvider<JSONObject> _dataProvider;
	private String _oldestSinceKey;
	private final SenseiSchema _senseiSchema;
	private final Configuration _myconfig;
  private final ApplicationContext _pluginContext;
	
	private Map<Integer, Zoie<BoboIndexReader, JSONObject>> _zoieSystemMap;
	private final LinkedHashMap<Integer, Collection<DataEvent<JSONObject>>> _dataCollectorMap;
  private final Comparator<String> _versionComparator;
    private final ShardingStrategy _shardingStrategy;
	
	public DefaultStreamingIndexingManager(SenseiSchema schema,Configuration senseiConfig, ApplicationContext pluginContext, Comparator<String> versionComparator){
	  _dataProvider = null;
	  _myconfig = senseiConfig.subset(CONFIG_PREFIX);
    _pluginContext = pluginContext;
	  _oldestSinceKey = null;
	  _senseiSchema = schema;
	  _zoieSystemMap = null;
	  _dataCollectorMap = new LinkedHashMap<Integer, Collection<DataEvent<JSONObject>>>();
    _versionComparator = versionComparator;
    
      String shardingStrategyName = _myconfig.getString(SHARDING_STRATEGY);
    
      ShardingStrategy strategy = null;
      
      if (shardingStrategyName!=null){
        strategy = (ShardingStrategy)pluginContext.getBean(shardingStrategyName);
      }
      if (strategy == null){
    	  strategy = new ShardingStrategy.FieldModShardingStrategy(_senseiSchema.getUidField());
      }
      
      _shardingStrategy = strategy;
	}

	public void updateOldestSinceKey(String sinceKey){
	    if(_oldestSinceKey == null){
	      _oldestSinceKey = sinceKey;
	    }
	    else if(sinceKey!=null && _versionComparator.compare(sinceKey, _oldestSinceKey) <0 ){
	      _oldestSinceKey = sinceKey;
	    }
	}
	
	@Override
	public void initialize(
			Map<Integer, Zoie<BoboIndexReader, JSONObject>> zoieSystemMap)
			throws Exception {

		int maxPartitionId = _myconfig.getInt(MAX_PARTITION_ID)+1;
		String uidField = _senseiSchema.getUidField();
		DataDispatcher consumer = new DataDispatcher(maxPartitionId,uidField);
		
		_zoieSystemMap = zoieSystemMap;
		
	    Iterator<Integer> it = zoieSystemMap.keySet().iterator();
	    while(it.hasNext()){
	      int part = it.next();
	      Zoie<BoboIndexReader,JSONObject> zoie = zoieSystemMap.get(part);
	      updateOldestSinceKey(zoie.getVersion());
	      _dataCollectorMap.put(part, new LinkedList<DataEvent<JSONObject>>());
	    }
	    
	    
	    _dataProvider = buildDataProvider();
	    _dataProvider.setDataConsumer(consumer);
	}
  
  @Override
  public DataProvider<JSONObject> getDataProvider()
  {
    return _dataProvider;
  }
	
	private StreamDataProvider<JSONObject> buildDataProvider() throws ConfigurationException{
		String type = _myconfig.getString(PROVIDER_TYPE);
		StreamDataProvider<JSONObject> dataProvider = null;

		Configuration conf = _myconfig.subset(type);
		
		SenseiGateway<?> builder = SenseiGatewayRegistry.getDataProviderBuilder(type);
		if (builder==null){
			builder = (SenseiGateway<?>)_pluginContext.getBean(type);
			if (builder == null){
			  throw new ConfigurationException("unsupported provider type: "+type);
			}
		}

		try{
		  dataProvider = builder.buildDataProvider(conf, _senseiSchema, _versionComparator, _oldestSinceKey, _pluginContext);
      long maxEventsPerMin = _myconfig.getLong(EVTS_PER_MIN,40000);
      dataProvider.setMaxEventsPerMinute(maxEventsPerMin);
      int batchSize = _myconfig.getInt(BATCH_SIZE,1);
      dataProvider.setBatchSize(batchSize);
		}
		catch(Exception e){
			throw new ConfigurationException(e.getMessage(),e);
		}
		
		try {
		   StandardMBean dataProviderMbean = new StandardMBean(new DataProviderAdmin(dataProvider), DataProviderAdminMBean.class);
		   JmxUtil.registerMBean(dataProviderMbean, "indexing-manager","stream-data-provider");
		} catch (Exception e) {
		  logger.error(e.getMessage(),e);
		} 
		return dataProvider;
	}

	@Override
	public void shutdown() {
	  _dataProvider.stop();
	}

	@Override
	public void start() throws Exception {
		if (_dataProvider==null){
			throw new Exception("data provider is not started");
		}
		_dataProvider.start();
	}

  @Override
  public void syncWithVersion(long timeToWait, String version) throws ZoieException
  {
    Iterator<Integer> itr = _zoieSystemMap.keySet().iterator();
    while (itr.hasNext())
    {
      int part_num = itr.next();
      Zoie<BoboIndexReader,JSONObject> dataConsumer = _zoieSystemMap.get(part_num);
      if (dataConsumer != null)
      {
        dataConsumer.syncWithVersion(timeToWait, version);
      }
    }
  }

  private class DataDispatcher implements DataConsumer<JSONObject>
  {
    int _maxPartitionId;  // the total number of partitions over all the nodes;
    private final String _uidField;
    private volatile String _currentVersion;
      
    public DataDispatcher(int maxPartitionId,String uidField){
      _maxPartitionId = maxPartitionId;
      _uidField = uidField;
      _currentVersion = null;
    }
      
    @Override
    public void consume(Collection<proj.zoie.api.DataConsumer.DataEvent<JSONObject>> data) throws ZoieException
    {
      try{
        for(DataEvent<JSONObject> dataEvt : data){
          JSONObject obj = dataEvt.getData();

          if (obj == null) // Just ignore this event.
            continue;

          _currentVersion = dataEvt.getVersion();

          int routeToPart = _shardingStrategy.caculateShard(_maxPartitionId, obj);
          if(DefaultStreamingIndexingManager.this._dataCollectorMap.containsKey(routeToPart)){
            Collection<DataEvent<JSONObject>> partDataSet = DefaultStreamingIndexingManager.this._dataCollectorMap.get(routeToPart);
            if (partDataSet!=null){
              partDataSet.add(dataEvt);
            }           
          }
        }
        
        Iterator<Integer> it = DefaultStreamingIndexingManager.this._zoieSystemMap.keySet().iterator();
        while(it.hasNext()){
          int part_num = it.next();
          Zoie<BoboIndexReader,JSONObject> dataConsumer = DefaultStreamingIndexingManager.this._zoieSystemMap.get(part_num);
          if (dataConsumer!=null){
            LinkedList<DataEvent<JSONObject>> partDataSet =
              (LinkedList<DataEvent<JSONObject>>) DefaultStreamingIndexingManager.this._dataCollectorMap.get(part_num);
            if (partDataSet != null)
            {
              if (partDataSet.size() == 0)
              {
                JSONObject markerObj = new JSONObject();
                markerObj.put(DefaultStreamingIndexingManager.this._senseiSchema.getSkipField(), "true");
                markerObj.put(_uidField, 0L); // Add a dummy uid
                partDataSet.add(new DataEvent<JSONObject>(markerObj, _currentVersion));
              }
              else if (_currentVersion != null && !_currentVersion.equals(partDataSet.getLast().getVersion()))
              {
                DataEvent<JSONObject> last = partDataSet.pollLast();
                partDataSet.add(new DataEvent<JSONObject>(last.getData(), _currentVersion));
              }
              dataConsumer.consume(partDataSet);
            }
          }
          DefaultStreamingIndexingManager.this._dataCollectorMap.put(part_num, new LinkedList<DataEvent<JSONObject>>());
        }
      }
      catch(Exception e){
        throw new ZoieException(e.getMessage(),e);
      }
    }
      
    @Override
    public String getVersion()
    {
      return _currentVersion;
    }
  }

}
