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
package com.senseidb.indexing.hadoop.util;

import java.io.IOException;

import org.apache.lucene.store.Directory;

/**
 * This class copies some methods from Lucene's SegmentInfos since that class
 * is not public.
 */
public final class LuceneUtil {

  static public final class IndexFileNames {
    /** Name of the index segment file */
    static public final String SEGMENTS = "segments";

    /** Name of the generation reference file name */
    static public final String SEGMENTS_GEN = "segments.gen";
  }

  /**
   * Check if the file is a segments_N file
   * @param name
   * @return true if the file is a segments_N file
   */
  public static boolean isSegmentsFile(String name) {
    return name.startsWith(IndexFileNames.SEGMENTS)
        && !name.equals(IndexFileNames.SEGMENTS_GEN);
  }

  /**
   * Check if the file is the segments.gen file
   * @param name
   * @return true if the file is the segments.gen file
   */
  public static boolean isSegmentsGenFile(String name) {
    return name.equals(IndexFileNames.SEGMENTS_GEN);
  }

  /**
   * Get the generation (N) of the current segments_N file in the directory.
   * 
   * @param directory -- directory to search for the latest segments_N file
   */
  public static long getCurrentSegmentGeneration(Directory directory)
      throws IOException {
    String[] files = directory.listAll();
    if (files == null)
      throw new IOException("cannot read directory " + directory
          + ": list() returned null");
    return getCurrentSegmentGeneration(files);
  }

  /**
   * Get the generation (N) of the current segments_N file from a list of
   * files.
   * 
   * @param files -- array of file names to check
   */
  public static long getCurrentSegmentGeneration(String[] files) {
    if (files == null) {
      return -1;
    }
    long max = -1;
    for (int i = 0; i < files.length; i++) {
      String file = files[i];
      if (file.startsWith(IndexFileNames.SEGMENTS)
          && !file.equals(IndexFileNames.SEGMENTS_GEN)) {
        long gen = generationFromSegmentsFileName(file);
        if (gen > max) {
          max = gen;
        }
      }
    }
    return max;
  }

  /**
   * Parse the generation off the segments file name and return it.
   */
  public static long generationFromSegmentsFileName(String fileName) {
    if (fileName.equals(IndexFileNames.SEGMENTS)) {
      return 0;
    } else if (fileName.startsWith(IndexFileNames.SEGMENTS)) {
      return Long.parseLong(
          fileName.substring(1 + IndexFileNames.SEGMENTS.length()),
          Character.MAX_RADIX);
    } else {
      throw new IllegalArgumentException("fileName \"" + fileName
          + "\" is not a segments file");
    }
  }

}
