package com.senseidb.federated.broker;


import com.senseidb.federated.broker.proxy.ProxyDataSource;
import com.senseidb.search.req.SenseiRequest;
import java.util.List;
import org.json.JSONObject;


/**
 * @author Dmytro Ivchenko
 */
public class MockDataSource implements ProxyDataSource
{
  private final List<JSONObject> data;
  public MockDataSource(List<JSONObject> data)
  {
    this.data = data;
  }

  @Override
  public List<JSONObject> getData(SenseiRequest senseiRequest)
  {
    return data;
  }
}
