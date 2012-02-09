package com.senseidb.search.req.mapred;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.json.JSONObject;

import proj.zoie.api.DocIDMapper;

import com.senseidb.search.req.mapred.CountGroupByMapReduce.GroupedValue;
import com.senseidb.search.req.mapred.CountGroupByMapReduce.IntContainer;
import com.senseidb.search.req.mapred.obsolete.MapReduceJob;

@SuppressWarnings("unchecked")
public class CountGroupByMapReduce implements MapReduceJob<HashMap<String, IntContainer>, ArrayList<GroupedValue>> {
  private static final long serialVersionUID = 1L;

  private final String[] columns;

  public CountGroupByMapReduce(String... columns) {
    this.columns = columns;
  }

  @Override
  public HashMap<String, IntContainer> map(long[] uids, DocIDMapper docIDMapper, FieldAccessor fieldAccessor) {
    HashMap<String, IntContainer> ret = new HashMap<String, IntContainer>();
    for (long uid : uids) {
      int docId = docIDMapper.quickGetDocID(uid);
      String key = getKey(columns, fieldAccessor, docId);
      IntContainer count = ret.get(key);
      if (!ret.containsKey(key)) {
        ret.put(key, new IntContainer(1));
      } else {
        count.add(1);
      }
    }
    return ret;
  }

  private String getKey(String[] columns, FieldAccessor fieldAccessor, int docId) {
    StringBuilder key = new StringBuilder(fieldAccessor.getString(columns[0], docId));
    for (int i = 1; i < columns.length; i++) {
      key.append(":").append(fieldAccessor.get(columns[i], docId).toString());
    }
    return key.toString();
  }

  @Override
  public List<HashMap<String, IntContainer>> combine(List<HashMap<String, IntContainer>> mapResults) {

    HashMap<String, IntContainer> ret = mapResults.get(0);
    for (int i = 1; i < mapResults.size(); i++) {
      HashMap<String, IntContainer> map = mapResults.get(i);
      for (String key : map.keySet()) {
        IntContainer count = ret.get(key);
        if (count != null) {
          count.add(map.get(key).value);
        } else {
          ret.put(key, map.get(key));
        }
      }
    }
    Iterator<IntContainer> iterator = ret.values().iterator();
    while (iterator.hasNext()) {
      if (iterator.next().value == 1)
        iterator.remove();
    }
    return java.util.Arrays.asList(ret);
  }

  @Override
  public ArrayList<GroupedValue> reduce(List<HashMap<String, IntContainer>> combineResults) {
    HashMap<String, IntContainer> retMap = combineResults.get(0);

    for (int i = 1; i < combineResults.size(); i++) {
      HashMap<String, IntContainer> map = combineResults.get(i);
      for (String key : map.keySet()) {
        IntContainer count = retMap.get(key);
        if (count != null) {
          count.add(map.get(key).value);
        } else {
          retMap.put(key, map.get(key));
        }
      }
    }
    ArrayList<GroupedValue> ret = new ArrayList<CountGroupByMapReduce.GroupedValue>();
    for (Map.Entry<String, IntContainer> entry : retMap.entrySet()) {
      ret.add(new GroupedValue(entry.getKey(), entry.getValue().value));
    }
    Collections.sort(ret);
    return ret;
  }

  public static class GroupedValue implements Comparable {
    String key;
    int value;

    public GroupedValue(String key, int value) {
      super();
      this.key = key;
      this.value = value;
    }

    @Override
    public int compareTo(Object o) {
      return ((GroupedValue) o).value - value;
    }

    @Override
    public String toString() {
      return key + ", count=" + value;

    }
  }

  public static class IntContainer implements Serializable {
    public int value;

    public IntContainer(int value) {
      super();
      this.value = value;
    }

    public IntContainer add(int value) {
      this.value += value;
      return this;
    }
  }

  public JSONObject render(ArrayList<GroupedValue> reduceResult) {
    // TODO Auto-generated method stub
    return null;
  }

}
