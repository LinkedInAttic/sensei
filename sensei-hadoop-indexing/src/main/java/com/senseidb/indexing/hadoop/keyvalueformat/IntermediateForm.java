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
package com.senseidb.indexing.hadoop.keyvalueformat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriter.MaxFieldLength;
import org.apache.lucene.index.KeepOnlyLastCommitDeletionPolicy;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.store.RAMDirectorySerializer;

import com.senseidb.indexing.hadoop.reduce.RAMDirectoryUtil;
import com.senseidb.indexing.hadoop.util.SenseiJobConfig;

/**
 * An intermediate form for one or more parsed Lucene documents and/or
 * delete terms. It actually uses Lucene file format as the format for
 * the intermediate form by using RAM dir files.
 * 
 * Note: If process(*) is ever called, closeWriter() should be called.
 * Otherwise, no need to call closeWriter().
 */
public class IntermediateForm implements Writable {

  private Configuration conf = null;
  private RAMDirectory dir;
  private IndexWriter writer;
  private int numDocs;

  /**
   * Constructor
   * @throws IOException
   */
  public IntermediateForm() throws IOException {
    dir = new RAMDirectory();
    writer = null;
    numDocs = 0;
  }

  /**
   * Configure using an index update configuration.
   * @param iconf  the index update configuration
   */
  public void configure(Configuration iconf) {
    this.conf = iconf;
  }

  /**
   * Get the ram directory of the intermediate form.
   * @return the ram directory
   */
  public Directory getDirectory() {
    return dir;
  }


  /**
   * This method is used by the index update mapper and process a document
   * operation into the current intermediate form.
   * @param doc  input document operation
   * @param analyzer  the analyzer
   * @throws IOException
   */
  public void process(Document doc, Analyzer analyzer) throws IOException {


      if (writer == null) {
        // analyzer is null because we specify an analyzer with addDocument
        writer = createWriter();
      }

      writer.addDocument(doc, analyzer);
      numDocs++;

  }

  /**
   * This method is used by the index update combiner and process an
   * intermediate form into the current intermediate form. More specifically,
   * the input intermediate forms are a single-document ram index and/or a
   * single delete term.
   * @param form  the input intermediate form
   * @throws IOException
   */
  public void process(IntermediateForm form) throws IOException {

    if (form.dir.sizeInBytes() > 0) {
      if (writer == null) {
        writer = createWriter();
      }

      writer.addIndexesNoOptimize(new Directory[] { form.dir });
      numDocs++;
    }

  }

  /**
   * Close the Lucene index writer associated with the intermediate form,
   * if created. Do not close the ram directory. In fact, there is no need
   * to close a ram directory.
   * @throws IOException
   */
  public void closeWriter() throws IOException {
    if (writer != null) {
      writer.optimize();
      writer.close();
      writer = null;
    }
  }

  /**
   * The total size of files in the directory and ram used by the index writer.
   * It does not include memory used by the delete list.
   * @return the total size in bytes
   */
  public long totalSizeInBytes() throws IOException {
    long size = dir.sizeInBytes();
    if (writer != null) {
      size += writer.ramSizeInBytes();
    }
    return size;
  }

  /* (non-Javadoc)
   * @see java.lang.Object#toString()
   */
  public String toString() {
    StringBuilder buffer = new StringBuilder();
    buffer.append(this.getClass().getSimpleName());
    buffer.append("[numDocs=");
    buffer.append(numDocs);
    buffer.append(", numDeletes=");
    buffer.append("]");
    return buffer.toString();
  }

  private IndexWriter createWriter() throws IOException {
    IndexWriter writer =
//        new IndexWriter(dir, false, null, new KeepOnlyLastCommitDeletionPolicy());
    	new IndexWriter(dir,  null, new KeepOnlyLastCommitDeletionPolicy(), MaxFieldLength.UNLIMITED);
    writer.setUseCompoundFile(true);  //use compound file fortmat to speed up;

    if (conf != null) {
      int maxFieldLength = conf.getInt(SenseiJobConfig.MAX_FIELD_LENGTH, -1);
      if (maxFieldLength > 0) {
        writer.setMaxFieldLength(maxFieldLength);
      }
    }

    return writer;
  }

  private void resetForm() throws IOException {
    if (dir.sizeInBytes() > 0) {
      // it's ok if we don't close a ram directory
      dir.close();
      // an alternative is to delete all the files and reuse the ram directory
      dir = new RAMDirectory();
    }
    assert (writer == null);
    numDocs = 0;
  }

  // ///////////////////////////////////
  // Writable
  // ///////////////////////////////////

  /* (non-Javadoc)
   * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
   */
  public void write(DataOutput out) throws IOException {

    String[] files = dir.listAll();
    RAMDirectoryUtil.writeRAMFiles(out, dir, files);
    
//    RAMDirectorySerializer.toDataOutput(out, dir);
  }

  /* (non-Javadoc)
   * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
   */
  public void readFields(DataInput in) throws IOException {
    resetForm();
    RAMDirectoryUtil.readRAMFiles(in, dir);

//	  numDocs = 0;
//	  dir = RAMDirectorySerializer.fromDataInput(in);
  }

}
