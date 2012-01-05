package com.sensei.search.util;

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
