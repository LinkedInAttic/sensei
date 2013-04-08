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
