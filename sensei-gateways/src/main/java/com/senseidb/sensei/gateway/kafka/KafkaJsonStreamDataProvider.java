package com.senseidb.sensei.gateway.kafka;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Comparator;

import org.json.JSONException;
import org.json.JSONObject;

import com.sensei.indexing.api.DataSourceFilter;
import com.sensei.indexing.api.DataSourceFilterable;

public class KafkaJsonStreamDataProvider extends KafkaStreamDataProvider<JSONObject> implements DataSourceFilterable<byte[]> {
    private final static Charset UTF8 = Charset.forName("UTF-8");
    public final static String KAFKA_MSG_OFFSET = "_KAFKA_MSG_OFFSET_";

    private DataSourceFilter<byte[]> _dataSourceFilter;
    
	public KafkaJsonStreamDataProvider(Comparator<String> versionComparator, String zookeeperUrl,
			int soTimeout, int batchSize, String consumerGroupId,String topic, long startingOffset) {
		super(versionComparator, zookeeperUrl, soTimeout, batchSize, consumerGroupId,topic, startingOffset);
	}

	@Override
    public void setFilter(DataSourceFilter<byte[]> filter)
    {
      _dataSourceFilter = filter;
    }

	@Override
	protected JSONObject convertMessageBytes(long msgStreamOffset, byte[] bytes,
			int offset, int size) throws IOException {
    try
    {
      if (_dataSourceFilter != null)
        return _dataSourceFilter.filter(Arrays.copyOfRange(bytes, offset, offset+size));
    }
    catch(Exception e)
    {
			throw new IOException(e.getMessage(),e);
    }

    // Try to create directly from message string.
		String jsonString = new String(bytes,offset,size,UTF8);
		
		try {
			JSONObject jsonObj = new JSONObject(jsonString);
			jsonObj.put(KAFKA_MSG_OFFSET, String.valueOf(msgStreamOffset));
			return jsonObj;
		} catch (JSONException e) {
			throw new IOException(e.getMessage(),e);
		}
	}

}
