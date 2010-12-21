package com.sensei.search.req.protobuf;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Map.Entry;

import org.apache.log4j.Logger;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.SortField;

import com.browseengine.bobo.api.BrowseFacet;
import com.browseengine.bobo.api.BrowseSelection;
import com.browseengine.bobo.api.FacetAccessible;
import com.browseengine.bobo.api.FacetIterator;
import com.browseengine.bobo.api.FacetSpec;
import com.browseengine.bobo.api.BrowseSelection.ValueOperation;
import com.browseengine.bobo.api.FacetSpec.FacetSortSpec;
import com.browseengine.bobo.facets.DefaultFacetHandlerInitializerParam;
import com.browseengine.bobo.facets.FacetHandlerInitializerParam;
import com.google.protobuf.ByteString;
import com.google.protobuf.TextFormat;
import com.google.protobuf.TextFormat.ParseException;
import com.sensei.search.req.SenseiHit;
import com.sensei.search.req.SenseiQuery;
import com.sensei.search.req.SenseiRequest;
import com.sensei.search.req.SenseiResult;

public class SenseiRequestBPOConverter {

	private static Logger logger = Logger.getLogger(SenseiRequestBPOConverter.class);
	
  public static SenseiRequest convert(SenseiRequestBPO.Request req)
  {
    try
    {
      ByteString value = req.getVal();
      byte[] raw = value.toByteArray();
      ByteArrayInputStream bais = new ByteArrayInputStream(raw);
      ObjectInputStream ois = new ObjectInputStream(bais);
      SenseiRequest ret = (SenseiRequest) ois.readObject();
      return ret;
    } catch (Exception e)
    {
      logger.error("serialize request", e);
    }
    return null;
  }
  public static SenseiRequestBPO.Request convert(SenseiRequest req)
  {
    SenseiRequestBPO.Request.Builder builder = SenseiRequestBPO.Request.newBuilder();
    try
    {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      ObjectOutputStream oos;
      oos = new ObjectOutputStream(baos);
      oos.writeObject(req);
      oos.close();
      byte[] raw = baos.toByteArray();
      builder.setVal(ByteString.copyFrom(raw));
      return builder.build();
    } catch (IOException e)
    {
      logger.error("deserialize request", e);
    }
    return SenseiRequestBPO.Request.getDefaultInstance();
  }
  public static SenseiResult convert(SenseiResultBPO.Result req)
  {
    try
    {
      ByteString value = req.getVal();
      byte[] raw = value.toByteArray();
      ByteArrayInputStream bais = new ByteArrayInputStream(raw);
      ObjectInputStream ois = new ObjectInputStream(bais);
      SenseiResult ret = (SenseiResult) ois.readObject();
      return ret;
    } catch (Exception e)
    {
      logger.error("serialize result", e);
    }
    return null;
  }
  public static SenseiResultBPO.Result convert(SenseiResult req)
  {
    SenseiResultBPO.Result.Builder builder = SenseiResultBPO.Result.newBuilder();
    try
    {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      ObjectOutputStream oos;
      oos = new ObjectOutputStream(baos);
      oos.writeObject(req);
      oos.close();
      byte[] raw = baos.toByteArray();
      builder.setVal(ByteString.copyFrom(raw));
      return builder.build();
    } catch (IOException e)
    {
      logger.error("deserialize result", e);
    }
    return SenseiResultBPO.Result.getDefaultInstance();
  }
}
