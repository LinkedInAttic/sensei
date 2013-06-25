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
package com.senseidb.search.req;


import com.linkedin.norbert.network.Serializer;
import com.senseidb.plugin.SenseiPluginFactory;
import com.senseidb.plugin.SenseiPluginRegistry;
import com.senseidb.svc.impl.CoreSenseiServiceImpl;

import java.util.Map;

public class SenseiJavaSerializer implements Serializer<SenseiRequest, SenseiResult>,
    SenseiPluginFactory<Serializer<SenseiRequest, SenseiResult>> {

  private final Serializer<SenseiRequest, SenseiResult> inner =
      CoreSenseiServiceImpl.JAVA_SERIALIZER;

  @Override
  public String requestName() {
    return inner.requestName();
  }

  @Override
  public SenseiRequest requestFromBytes(byte[] bytes) {
    return inner.requestFromBytes(bytes);
  }

  @Override
  public SenseiResult responseFromBytes(byte[] bytes) {
    return inner.responseFromBytes(bytes);
  }

  @Override
  public String responseName() {
    return inner.responseName();
  }

  @Override
  public byte[] requestToBytes(SenseiRequest senseiRequest) {
    return inner.requestToBytes(senseiRequest);
  }

  @Override
  public byte[] responseToBytes(SenseiResult senseiResult) {
    return inner.responseToBytes(senseiResult);
  }

  @Override
  public Serializer<SenseiRequest, SenseiResult> getBean(Map<String, String> initProperties, String fullPrefix, SenseiPluginRegistry pluginRegistry) {
    return new SenseiJavaSerializer();
  }
}
