package com.senseidb.gateway.kafka;

import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.json.JSONObject;

import com.senseidb.indexing.DataSourceFilter;

public class AvroDataSourceFilter<D> extends DataSourceFilter<DataPacket> {

  private final Class<D> _cls;
  private BinaryDecoder binDecoder;
  private final SpecificDatumReader<D> reader;
  private final DataSourceFilter<D> _dataMapper;
  
  public AvroDataSourceFilter(Class<D> cls,DataSourceFilter<D> dataMapper){
    _cls = cls;
    binDecoder = null;
    reader = new SpecificDatumReader<D>(_cls);
    _dataMapper = dataMapper;
    if (_dataMapper == null){
      throw new IllegalArgumentException("source filter is null");
    }
  }
  
  @Override
  protected JSONObject doFilter(DataPacket packet) throws Exception {
    binDecoder = DecoderFactory.defaultFactory().createBinaryDecoder(packet.data,packet.offset,packet.size, binDecoder);
    D avroObj = _cls.newInstance();
    reader.read(avroObj,binDecoder);
    return _dataMapper.filter(avroObj);
  }
}
