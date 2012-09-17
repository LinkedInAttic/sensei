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
