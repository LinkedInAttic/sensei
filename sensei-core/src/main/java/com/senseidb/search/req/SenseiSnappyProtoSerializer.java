package com.senseidb.search.req;


import com.linkedin.norbert.network.Serializer;
import com.senseidb.plugin.SenseiPluginFactory;
import com.senseidb.plugin.SenseiPluginRegistry;

import java.util.Map;

/**
 * A serializer that will use snappy compression & straightforward protocol buffer serialization to pass data
 * between broker & searcher. Intended to be very high performance
 */
public class SenseiSnappyProtoSerializer extends SenseiSnappySerializer<SenseiRequest, SenseiResult>
  implements SenseiPluginFactory<Serializer<SenseiRequest, SenseiResult>> {

  @Override
  public Serializer<SenseiRequest, SenseiResult> getBean(Map<String, String> initProperties, String fullPrefix, SenseiPluginRegistry pluginRegistry) {
    return new SenseiSnappyProtoSerializer();
  }

  public SenseiSnappyProtoSerializer() {
    super(new SenseiRequestProtoSerializer());
  }
}
