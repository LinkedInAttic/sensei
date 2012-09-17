/**
 * This software is licensed to you under the Apache License, Version 2.0 (the
 * "Apache License").
 *
 * LinkedIn's contributions are made under the Apache License. If you contribute
 * to the Software, the contributions will be deemed to have been made under the
 * Apache License, unless you expressly indicate otherwise. Please do not make any
 * contributions that would be inconsistent with the Apache License.
 *
 * You may obtain a copy of the Apache License at http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, this software
 * distributed under the Apache License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the Apache
 * License for the specific language governing permissions and limitations for the
 * software governed under the Apache License.
 *
 * © 2012 LinkedIn Corp. All Rights Reserved.  
 */
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
