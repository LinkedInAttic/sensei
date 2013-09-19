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

import java.io.ByteArrayOutputStream;

import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

public class AvroSerializerHelper {

	public static <V> byte[] toBytes(V v,Class<V> cls) throws Exception{
        ByteArrayOutputStream bout = new ByteArrayOutputStream();
		
		SpecificDatumWriter<V> writer = new SpecificDatumWriter<V>(cls);
		BinaryEncoder binEncoder = new BinaryEncoder(bout);
		writer.write(v, binEncoder);
		binEncoder.flush();
		
		return bout.toByteArray();
	}
	
	public static <V> V fromBytes(byte[] bytes, Class<V> cls) throws Exception{
       SpecificDatumReader<V> reader = new SpecificDatumReader<V>(cls);
		
		BinaryDecoder binDecoder = DecoderFactory.defaultFactory().createBinaryDecoder(bytes, null);
		V val = cls.newInstance();
		reader.read(val, binDecoder);
		
		return val;
	}
}
