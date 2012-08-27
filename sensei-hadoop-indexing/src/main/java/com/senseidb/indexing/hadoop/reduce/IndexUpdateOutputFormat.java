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

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Progressable;

import com.senseidb.indexing.hadoop.keyvalueformat.Shard;

/**
 * The record writer of this output format simply puts a message in an output
 * path when a shard update is done.
 */
public class IndexUpdateOutputFormat extends FileOutputFormat<Shard, Text> {

  static final Text DONE = new Text("done");
	
  public RecordWriter<Shard, Text> getRecordWriter(final FileSystem fs,
      JobConf job, String name, final Progressable progress)
      throws IOException {

    final Path perm = new Path(getWorkOutputPath(job), name);

    return new RecordWriter<Shard, Text>() {
      public void write(Shard key, Text value) throws IOException {
        assert (DONE.equals(value));

        String shardName = key.getDirectory();
        shardName = shardName.replace("/", "_");

        Path doneFile =
            new Path(perm, DONE + "_" + shardName);
        if (!fs.exists(doneFile)) {
          fs.createNewFile(doneFile);
        }
      }

      public void close(final Reporter reporter) throws IOException {
      }
    };
  }
}
