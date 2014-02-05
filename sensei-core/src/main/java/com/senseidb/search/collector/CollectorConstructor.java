package com.senseidb.search.collector;

import com.senseidb.util.ObjectContructorUtil;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Searchable;
import org.json.JSONObject;

/**
 * Utility method to parse the Json request and construct a
 * custom collector object.
 * @author darya
 */
public class CollectorConstructor {

  public static Collector constructCollector(JSONObject collector,
                                             QueryParser qparser,
                                             Query q,
                                             Searchable searchable) {

    String className;
    Object o = null;

    try {
      className = collector.getString(ObjectContructorUtil.TYPE_CLASS);
      o = ObjectContructorUtil.constructObject(className, collector, qparser, q, searchable);

    } catch (Exception e) {
      e.printStackTrace();
    }

    return (Collector)o;
  }

}
