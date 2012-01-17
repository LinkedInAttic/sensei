package com.senseidb.cluster.routing;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import org.apache.log4j.Logger;

public class MD5HashProvider implements HashProvider
{
  private final static Logger logger = Logger.getLogger(MD5HashProvider.class);
  private final ThreadLocal<MessageDigest> _md = new ThreadLocal<MessageDigest>()
  {
    protected MessageDigest initialValue()
    {
      try
      {
        return MessageDigest.getInstance("MD5");
      } catch (NoSuchAlgorithmException e)
      {
        logger.error(e);
      }
      return null;
    }
  };

  /**
   * Hash the key into an integer.
   * 
   * @param key
   *          the key to be hashed
   * @return the hash code of the key
   */
  public long hash(String key)
  {
    byte[] kbytes = _md.get().digest(key.getBytes());
    long hc = ((long) (kbytes[3] & 0xFF) << 24) | ((long) (kbytes[2] & 0xFF) << 16) | ((long) (kbytes[1] & 0xFF) << 8) | (long) (kbytes[0] & 0xFF);
    _md.get().reset();
    return Math.abs(hc);
  }

}
