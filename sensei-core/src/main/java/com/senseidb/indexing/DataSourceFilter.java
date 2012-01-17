package com.senseidb.indexing;

import org.apache.commons.codec.binary.Base64;
import org.json.JSONObject;

public abstract class DataSourceFilter<D>
{
  protected String _srcDataStore;
  protected String _srcDataField = "src_data";

  protected abstract JSONObject doFilter(D data) throws Exception;

  public JSONObject filter(D data) throws Exception
  {
    JSONObject obj = doFilter(data);
    if (data != null && obj != null && !obj.has(_srcDataField) && _srcDataStore != null && _srcDataStore.length() != 0 &&
        !"none".equals(_srcDataStore) && _srcDataField != null && _srcDataField.length() != 0)
    {
      if (data instanceof byte[])
      {
        obj.put(_srcDataField, Base64.encodeBase64String((byte[])data));
      }
      else
      {
        obj.put(_srcDataField, data.toString());
      }
    }
    return obj;
  }

  public void setSrcDataStore(String srcDataStore)
  {
    _srcDataStore = srcDataStore;
  }

  public void setSrcDataField(String srcDataField)
  {
    _srcDataField = srcDataField;
  }
}

