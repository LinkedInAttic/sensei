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
package com.senseidb.indexing.activity;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;

/**
 * Keeps the persisted metadata info: last persisted version  and document count. Uses two files to ensure data integrity
 * @author vzhabiuk
 *
 */
public class Metadata {
  public volatile String version;
  public volatile int count;
  private final String indexDir; 
  private File file1;
  private File file2;
  public Metadata(String indexDir) {
    super();
    this.indexDir = indexDir;      
  }
  public void init() {
    try { 
      file1 = new File(indexDir, "metadata1");
    
    file2 = new File(indexDir, "metadata2");
    if (!file1.exists()) {
      file1.createNewFile();
    }
    if (!file2.exists()) {
      file2.createNewFile();
    } else {
      long modifiedTime1 = file1.lastModified();
      long modifiedTime2 = file2.lastModified();
      if (modifiedTime1 > modifiedTime2) {
        init(FileUtils.readFileToString(file2));
      } else {
        init(FileUtils.readFileToString(file1));
      }
    }  
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }
  public void update(String version, int count)  {
    this.version = version;
    this.count = count;
    try {
    FileUtils.writeStringToFile(file1, this.toString());
    FileUtils.writeStringToFile(file2, this.toString());
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }
  }
  @Override
  public String toString() {
   return version + ";" + count;
  }
  protected void init(String str) {
    if (!str.contains(";")) {
      return ;
    }
    version = str.split(";")[0];
    count = Integer.parseInt(str.split(";")[1]);     
  }
}
