package com.senseidb.search.relevance;

import java.util.Map;
import java.util.Set;

public interface CustomScorer
{

  // Javassist does not support inner class or interface, so put it here;
  float score(short[] shorts, int[] ints, long[] longs, float[] floats, double[] doubles, boolean[] booleans, String[] strings, Set[] sets, Map[] maps,
              MFacetInt[] mFacetInts, MFacetLong[] mFacetLongs, MFacetFloat[] mFacetFloats, MFacetDouble[] mFacetDoubles, MFacetShort[] mFacetShorts, MFacetString[] mFacetStrings); 
  
}
