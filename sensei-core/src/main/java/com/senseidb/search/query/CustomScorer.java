package com.senseidb.search.query;

public interface CustomScorer
{

  // Javassist does not support inner class or interface, so put it here;
  float score(Object[] objs);
}
