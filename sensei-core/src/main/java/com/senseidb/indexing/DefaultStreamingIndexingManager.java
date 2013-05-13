package com.senseidb.indexing;

import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import javax.management.StandardMBean;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.log4j.Logger;
import org.json.JSONObject;

import proj.zoie.api.DataConsumer;
import proj.zoie.api.DataConsumer.DataEvent;
import proj.zoie.api.DataProvider;
import proj.zoie.api.Zoie;
import proj.zoie.api.ZoieException;
import proj.zoie.api.ZoieIndexReader;
import proj.zoie.impl.indexing.StreamDataProvider;
import proj.zoie.impl.indexing.ZoieConfig;
import proj.zoie.mbean.DataProviderAdmin;
import proj.zoie.mbean.DataProviderAdminMBean;

import com.browseengine.bobo.api.BoboIndexReader;
import com.senseidb.conf.SenseiSchema;
import com.senseidb.gateway.SenseiGateway;
import com.senseidb.indexing.activity.CompositeActivityManager;
import com.senseidb.jmx.JmxUtil;
import com.senseidb.metrics.MetricsConstants;
import com.senseidb.plugin.SenseiPluginRegistry;
import com.senseidb.search.node.SenseiIndexingManager;
import com.senseidb.search.plugin.PluggableSearchEngineManager;
import com.senseidb.util.JSONUtil.FastJSONArray;
import com.senseidb.util.JSONUtil.FastJSONObject;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Timer;
import com.yammer.metrics.core.Meter;
import com.yammer.metrics.core.MetricName;

public class DefaultStreamingIndexingManager implements SenseiIndexingManager<JSONObject> {

	private static final Logger logger = Logger.getLogger(DefaultStreamingIndexingManager.class);

	public static final String CONFIG_PREFIX = "sensei.index.manager.default";

	private static final String MAX_PARTITION_ID = "maxpartition.id";

	private static final String EVTS_PER_MIN = "eventsPerMin";

	private static final String BATCH_SIZE = "batchSize";

  private static final String EVENT_CREATED_TIMESTAMP_FIELD = "eventCreatedTimestampField"; 

  private static Meter ProviderBatchSizeMeter = null;
  private static Meter EventMeter = null;
  private static Meter UpdateBatchSizeMeter = null;
  private static Timer IndexingLatencyTimer = null;

  static{
    try{
      MetricName providerBatchSizeMetricName = new MetricName(MetricsConstants.Domain,"meter","provider-batch-size","indexing-manager");
      ProviderBatchSizeMeter = Metrics.newMeter(providerBatchSizeMetricName,"provide-batch-size", TimeUnit.SECONDS);

      MetricName updateBatchSizeMetricName = new MetricName(MetricsConstants.Domain,"meter","update-batch-size","indexing-manager");
      UpdateBatchSizeMeter = Metrics.newMeter(updateBatchSizeMetricName,"update-batch-size", TimeUnit.SECONDS);

      MetricName eventMeterMetricName = new MetricName(MetricsConstants.Domain,"meter","indexing-events","indexing-manager");
      EventMeter = Metrics.newMeter(eventMeterMetricName, "indexing-events", TimeUnit.SECONDS);

      MetricName indexingLatencyMetricName = new MetricName(MetricsConstants.Domain,
                                                            "timer",
                                                            "indexing-latency",
                                                            "indexing-manager");
      IndexingLatencyTimer = Metrics.newTimer(indexingLatencyMetricName,
                                              TimeUnit.MILLISECONDS,
                                              TimeUnit.SECONDS);
    }
    catch(Exception e){
    logger.error(e.getMessage(),e);
    }
  }


	private StreamDataProvider<JSONObject> _dataProvider;
	private String _oldestSinceKey;
  private String _eventCreatedTimestampField;
	private final SenseiSchema _senseiSchema;
	private final Configuration _myconfig;

	private Map<Integer, Zoie<BoboIndexReader, JSONObject>> _zoieSystemMap;
	private final LinkedHashMap<Integer, Collection<DataEvent<JSONObject>>> _dataCollectorMap;

	private final SenseiGateway<?> _gateway;
  private final ShardingStrategy _shardingStrategy;
  private final Comparator<String> _versionComparator;
  private final PluggableSearchEngineManager pluggableSearchEngineManager;
  private SenseiPluginRegistry pluginRegistry;
  

  

	public DefaultStreamingIndexingManager(SenseiSchema schema,Configuration senseiConfig, 
	    SenseiPluginRegistry pluginRegistry, SenseiGateway<?> gateway, ShardingStrategy shardingStrategy, PluggableSearchEngineManager pluggableSearchEngineManager){
	    _dataProvider = null;
	  _myconfig = senseiConfig.subset(CONFIG_PREFIX);
    _eventCreatedTimestampField = _myconfig.getString(EVENT_CREATED_TIMESTAMP_FIELD, null);
     this.pluginRegistry = pluginRegistry;
	  _oldestSinceKey = null;
	  _senseiSchema = schema;
	  _zoieSystemMap = null;
	  _dataCollectorMap = new LinkedHashMap<Integer, Collection<DataEvent<JSONObject>>>();
	  _gateway = gateway;
	  this.pluggableSearchEngineManager = pluggableSearchEngineManager;
	  if (_gateway!=null){
	    _versionComparator = _gateway.getVersionComparator();
	  }
	  else{
	    _versionComparator = ZoieConfig.DEFAULT_VERSION_COMPARATOR;
	  }
    _shardingStrategy = shardingStrategy;
	}

	public void updateOldestSinceKey(String sinceKey){
	    if(_oldestSinceKey == null){
	      _oldestSinceKey = sinceKey;
	      if (_dataProvider != null) {
	        _dataProvider.setStartingOffset(_oldestSinceKey);
	      }
	    }
	    else if(sinceKey!=null && _versionComparator.compare(sinceKey, _oldestSinceKey) <0 ){
	      _oldestSinceKey = sinceKey;
	      if (_dataProvider != null) {
	        _dataProvider.setStartingOffset(_oldestSinceKey);
	      }
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

	    if (pluggableSearchEngineManager != null && pluggableSearchEngineManager.getOldestVersion() != null && !("".equals(pluggableSearchEngineManager.getOldestVersion()))) {
	      updateOldestSinceKey(pluggableSearchEngineManager.getOldestVersion());	    
	    }

      _dataProvider = buildDataProvider();

	    if (_dataProvider!=null){
	    _dataProvider.setDataConsumer(consumer);
	    }	   
	}

  @Override
  public DataProvider<JSONObject> getDataProvider()
  {
    return _dataProvider;
  }

	private StreamDataProvider<JSONObject> buildDataProvider() throws ConfigurationException{
		StreamDataProvider<JSONObject> dataProvider = null;
    if (_gateway!=null){
		  try{
		    dataProvider = _gateway.buildDataProvider(_senseiSchema, _oldestSinceKey, pluginRegistry,_shardingStrategy,_zoieSystemMap.keySet());
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
    }
		return dataProvider;
	}

	@Override
	public void shutdown() {
	  if (pluggableSearchEngineManager != null) {
	    pluggableSearchEngineManager.close();
	  }
	  if (_dataProvider!=null){
	    _dataProvider.stop();
	  }
	}

	@Override
	public void start() throws Exception {
		if (_dataProvider==null){
		  logger.warn("no data stream configured, no indexing events are flowing.");
		}
		else{
	 	  _dataProvider.start();
		}
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

    private void reportIndexingLatency(JSONObject obj)
    {
      if (_eventCreatedTimestampField != null)
      {
        long createdTimestamp = obj.optLong(_eventCreatedTimestampField);
        if (createdTimestamp > 0)
        {
          IndexingLatencyTimer.update(System.currentTimeMillis() - createdTimestamp,
                                      TimeUnit.MILLISECONDS);
        }
      }
    }

    private JSONObject rewriteData(JSONObject obj, int partNum)
    {
      String type = obj.optString(SenseiSchema.EVENT_TYPE_FIELD, null);

      JSONObject event = obj.optJSONObject(SenseiSchema.EVENT_FIELD);
      if (event == null)
        event = obj;
      else if (type != null)
      {
        try
        {
          event.put(SenseiSchema.EVENT_TYPE_FIELD, type);
        }
        catch(Exception e)
        {
          logger.error("Should never happen", e);
        }
      }

      reportIndexingLatency(event);

      if (SenseiSchema.EVENT_TYPE_UPDATE.equalsIgnoreCase(type))
      {
        Zoie<BoboIndexReader, JSONObject> zoie = _zoieSystemMap.get(partNum);
        List<ZoieIndexReader<BoboIndexReader>> readers;
        try
        {
          readers = zoie.getIndexReaders();
        }
        catch(Exception e)
        {
          logger.error(e.getMessage(), e);
          return null;
        }

        if (readers == null)
        {
          logger.error("Cannot found original doc for and update event: " + obj);
          return null;
        }
        try
        {
          byte[] src = null;
          long uid = Long.parseLong(event.getString(_senseiSchema.getUidField()));
          for (ZoieIndexReader<BoboIndexReader> reader : readers)
          {            
            src = reader.getStoredValue(uid);
            if (src != null)
              break;
          }          
          byte[] data = null;

          if (_senseiSchema.isCompressSrcData())
            data = DefaultJsonSchemaInterpreter.decompress(src);
          else
            data = src;

          if (data == null)
          {
            logger.error("Cannot found original doc for and update event: " + obj);
            return null;
          }

          JSONObject newEvent = new FastJSONObject(new String(data, "UTF-8"));
          Iterator<String> keys = event.keys();
          while(keys.hasNext())
          {
            String key = keys.next();
            newEvent.put(key, event.get(key));
          }
          event = newEvent;
        }
        catch (Exception e)
        {
          logger.error(e.getMessage(), e);
          return null;
        }
        finally
        {
          zoie.returnIndexReaders(readers);
        }
      }

      return event;
    }

    @Override
    public void consume(Collection<proj.zoie.api.DataConsumer.DataEvent<JSONObject>> data) throws ZoieException
    {
      UpdateBatchSizeMeter.mark(data.size());
      ProviderBatchSizeMeter.mark(_dataProvider.getBatchSize());
      EventMeter.mark(_dataProvider.getEventCount());

      try{
        for(DataEvent<JSONObject> dataEvt : data){
          JSONObject obj = dataEvt.getData();

          if (obj == null) // Just ignore this event.
            continue;

          _currentVersion = dataEvt.getVersion();
          if (pluggableSearchEngineManager != null && pluggableSearchEngineManager.acceptEventsForAllPartitions()) {
            obj = pluggableSearchEngineManager.update(obj, _currentVersion);
          }
          int routeToPart = _shardingStrategy.caculateShard(_maxPartitionId, obj);
          Collection<DataEvent<JSONObject>> partDataSet = _dataCollectorMap.get(routeToPart);
          if (partDataSet != null)
          {
            JSONObject rewrited = obj;
            if (pluggableSearchEngineManager != null && !pluggableSearchEngineManager.acceptEventsForAllPartitions()) {
              rewrited = pluggableSearchEngineManager.update(obj, dataEvt.getVersion());
            }
            rewrited = rewriteData(obj, routeToPart);
            if (rewrited != null)
            {
              
              if (rewrited != obj)
                dataEvt = new DataEvent<JSONObject>(rewrited, dataEvt.getVersion());
              partDataSet.add(dataEvt);
            }
          }
        }

        Iterator<Integer> it = _zoieSystemMap.keySet().iterator();
        while(it.hasNext()){
          int part_num = it.next();
          Zoie<BoboIndexReader,JSONObject> dataConsumer = _zoieSystemMap.get(part_num);
          if (dataConsumer!=null){
            LinkedList<DataEvent<JSONObject>> partDataSet =
              (LinkedList<DataEvent<JSONObject>>) _dataCollectorMap.get(part_num);
            if (partDataSet != null)
            {
              if (partDataSet.size() == 0)
              {
                JSONObject markerObj = new FastJSONObject();
                //markerObj.put(_senseiSchema.getSkipField(), "true");
                markerObj.put(SenseiSchema.EVENT_TYPE_FIELD, SenseiSchema.EVENT_TYPE_SKIP);
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
          _dataCollectorMap.put(part_num, new LinkedList<DataEvent<JSONObject>>());
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

    @Override
    public Comparator<String> getVersionComparator() {
      return _versionComparator;
    }
  }
}
