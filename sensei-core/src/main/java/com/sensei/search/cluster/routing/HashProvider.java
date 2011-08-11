package com.sensei.search.cluster.routing;

public interface HashProvider
{
  /**
   * Hash the key into a long.
   * 
   * @param key
   *          the key to be hashed
   * @return the hash code of the key
   */
  public long hash(String key);

}
