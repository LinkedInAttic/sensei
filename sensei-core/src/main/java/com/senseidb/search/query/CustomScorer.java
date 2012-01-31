package com.senseidb.search.query;

import java.util.Set;

public interface CustomScorer
{

  // Javassist does not support inner class or interface, so put it here;
  float score(short[] shorts, int[] ints, long[] longs, float[] floats, double[] doubles, boolean[] booleans, String[] strings, Set[] sets); 
  
}
