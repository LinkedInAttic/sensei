/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
