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
package com.senseidb.indexing.hadoop.reduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.RAMDirectory;

/**
 * A utility class which writes an index in a ram dir into a DataOutput and
 * read from a DataInput an index into a ram dir.
 */
public class RAMDirectoryUtil {
  private static final int BUFFER_SIZE = 1024; // RAMOutputStream.BUFFER_SIZE;

  /**
   * Write a number of files from a ram directory to a data output.
   * @param out  the data output
   * @param dir  the ram directory
   * @param names  the names of the files to write
   * @throws IOException
   */
  public static void writeRAMFiles(DataOutput out, RAMDirectory dir,
      String[] names) throws IOException {
    out.writeInt(names.length);

    for (int i = 0; i < names.length; i++) {
      Text.writeString(out, names[i]);
      long length = dir.fileLength(names[i]);
      out.writeLong(length);

      if (length > 0) {
        // can we avoid the extra copy?
        IndexInput input = null;
        try {
          input = dir.openInput(names[i], BUFFER_SIZE);

          int position = 0;
          byte[] buffer = new byte[BUFFER_SIZE];

          while (position < length) {
            int len =
                position + BUFFER_SIZE <= length ? BUFFER_SIZE
                    : (int) (length - position);
            input.readBytes(buffer, 0, len);
            out.write(buffer, 0, len);
            position += len;
          }
        } finally {
          if (input != null) {
            input.close();
          }
        }
      }
    }
  }

  /**
   * Read a number of files from a data input to a ram directory.
   * @param in  the data input
   * @param dir  the ram directory
   * @throws IOException
   */
  public static void readRAMFiles(DataInput in, RAMDirectory dir)
      throws IOException {
    int numFiles = in.readInt();

    for (int i = 0; i < numFiles; i++) {
      String name = Text.readString(in);
      long length = in.readLong();

      if (length > 0) {
        // can we avoid the extra copy?
        IndexOutput output = null;
        try {
          output = dir.createOutput(name);

          int position = 0;
          byte[] buffer = new byte[BUFFER_SIZE];

          while (position < length) {
            int len =
                position + BUFFER_SIZE <= length ? BUFFER_SIZE
                    : (int) (length - position);
            in.readFully(buffer, 0, len);
            output.writeBytes(buffer, 0, len);
            position += len;
          }
        } finally {
          if (output != null) {
            output.close();
          }
        }
      }
    }
  }

}
