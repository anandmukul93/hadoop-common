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
package org.apache.hadoop.mapreduce.lib.ramp.file;

import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * The ReduceProvenanceFormat class
 */
public class ReduceProvenanceFormat
    extends FileOutputFormat<LongWritable, LongWritable> {

  protected static final String COMPACT_OUTPUT =
    "mapreduce.ramp.job.provenance.outputformat.compact";

  protected static class FixedLengthRecordWriter
      extends RecordWriter<LongWritable, LongWritable> {

    protected DataOutputStream out;

    protected FixedLengthRecordWriter(DataOutputStream out) {
      this.out = out;
    }

    @Override
    public void write(LongWritable key, LongWritable value)
        throws IOException {
      out.writeInt((int) key.get());
      out.writeLong(value.get());
    }

    @Override
    public void close(TaskAttemptContext context) throws IOException {
      out.close();
    }
  }

  protected static class VariableLengthRecordWriter
      extends FixedLengthRecordWriter {

    private static final int BLK_SIZE = 8192;

    protected VariableLengthRecordWriter(DataOutputStream out) {
      super(out);
    }

    @Override
    public void write(LongWritable key, LongWritable value)
        throws IOException {
      int freeBytesInBlock =
        BLK_SIZE - (int)(((FSDataOutputStream) out).getPos() & (BLK_SIZE - 1));
      if (freeBytesInBlock < 18) {
        for (int i = 0; i < freeBytesInBlock; ++i) {
          out.writeByte(0xad);
        }
      }
      WritableUtils.writeVLong(out, key.get());
      WritableUtils.writeVLong(out, value.get());
    }
  }

  public static boolean getCompactOutput(JobContext job) {
    return job.getConfiguration().getBoolean(COMPACT_OUTPUT, true);
  }

  public static void setCompactOutput(JobContext job, boolean compact) {
    job.getConfiguration().setBoolean(COMPACT_OUTPUT, compact);
  }

  @Override
  public RecordWriter<LongWritable, LongWritable>
      getRecordWriter(TaskAttemptContext job)
      throws IOException, InterruptedException {
    Configuration conf = job.getConfiguration();
    Path file = getDefaultWorkFile(job, "");

    FileSystem fs = file.getFileSystem(conf);
    FSDataOutputStream fileOut = fs.create(file, false);

    if (getCompactOutput(job)) {
      return new VariableLengthRecordWriter(fileOut);
    } else {
      return new FixedLengthRecordWriter(fileOut);
    }
  }

}
