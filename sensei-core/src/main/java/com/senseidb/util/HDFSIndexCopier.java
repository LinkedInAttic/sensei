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
package com.senseidb.util;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import proj.zoie.api.IndexCopier;

public class HDFSIndexCopier implements IndexCopier
{
  public static final Logger log = Logger.getLogger(HDFSIndexCopier.class);

  public boolean copy(String src, String dest)
  {
    try
    {
      URI srcUri = new URI(src), destUri = new URI(dest);

      Configuration config = new Configuration();
      config.set("fs.default.name", srcUri.resolve("/").toString());

      FileSystem dfs = FileSystem.get(config);
      Path destPath = new Path(destUri.toString());
      FileStatus[] files = dfs.listStatus(new Path(srcUri.toString()));
      if (files == null || files.length == 0)
        return false;

      for (FileStatus f : files)
      {
        log.info("Copying " + f.getPath().toString());
        dfs.copyToLocalFile(f.getPath(), destPath);
      }

      return true;
    }
    catch(Exception e)
    {
      log.error(e.getMessage(), e);
      return false;
    }
  }
}
