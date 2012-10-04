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

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.lucene.index.IndexFileNameFilter;

/**
 * A wrapper class to convert an IndexFileNameFilter which implements
 * java.io.FilenameFilter to an org.apache.hadoop.fs.PathFilter.
 */
public class LuceneIndexFileNameFilter implements PathFilter {

  private static final LuceneIndexFileNameFilter singleton =
      new LuceneIndexFileNameFilter();

  /**
   * Get a static instance.
   * @return the static instance
   */
  public static LuceneIndexFileNameFilter getFilter() {
    return singleton;
  }

  private final IndexFileNameFilter luceneFilter;

  private LuceneIndexFileNameFilter() {
    luceneFilter = IndexFileNameFilter.getFilter();
  }

  /* (non-Javadoc)
   * @see org.apache.hadoop.fs.PathFilter#accept(org.apache.hadoop.fs.Path)
   */
  public boolean accept(Path path) {
    return luceneFilter.accept(null, path.getName());
  }

}
