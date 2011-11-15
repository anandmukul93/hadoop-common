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
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * The MapProvenanceFormat class
 */
public class MapProvenanceFormat
    extends FileOutputFormat<LongWritable, FilePosition> {

  protected static final String COMPACT_OUTPUT =
    "mapreduce.ramp.map.provenance.outputformat.compact";
  protected static final String RAMP_DICT = "_ramp_dict";

  protected static class FixedLengthRecordWriter
      extends RecordWriter<LongWritable, FilePosition> {

    private static final String utf8 = "UTF-8";
    private static final byte[] newline;
    private static final byte[] tab;
    static {
      try {
        newline = "\n".getBytes(utf8);
        tab = "\t".getBytes(utf8);
      } catch (UnsupportedEncodingException uee) {
        throw new IllegalArgumentException("can't find " + utf8 + " encoding");
      }
    }

    protected DataOutputStream out;
    private DataOutputStream dictOut;
    private Path[] inputDirs;
    private ArrayList<HashMap<Text, Integer>> fileToFileId;
    private Integer nextFileId = new Integer(0);

    protected FixedLengthRecordWriter(
        DataOutputStream out, DataOutputStream dictOut, JobContext context) {
      this.out = out;
      this.dictOut = dictOut;
      inputDirs = FileInputFormat.getInputPaths(context);
      fileToFileId = new ArrayList<HashMap<Text, Integer>>(inputDirs.length);
      for (int i = 0; i < inputDirs.length + 1; ++i) {
        fileToFileId.add(new HashMap<Text, Integer>());
      }
    }

    @Override
    public void write(LongWritable key, FilePosition value)
        throws IOException {
      Text file = value.getFile();
      Integer fileId = fileToFileId.get(value.getDir() + 1).get(file);

      if (fileId == null) {
        dictOut.write(nextFileId.toString().getBytes());
        dictOut.write(tab);
        if (value.getDir() >= 0) {
          String path =
            new Path(inputDirs[value.getDir()], file.toString()).toString();
          dictOut.write(path.getBytes(), 0, path.length());
        } else {
          dictOut.write(file.getBytes(), 0, file.getLength());
        }
        dictOut.write(newline);

        fileToFileId.get(value.getDir() + 1).put(file, nextFileId);
        fileId = nextFileId;
        nextFileId = new Integer(nextFileId.intValue() + 1);
      }

      writeRecord(key.get(), fileId.intValue(), value.getPosition());
    }

    protected void writeRecord(long id, int fileId, long position)
        throws IOException {
      out.writeInt((int) id);
      out.writeInt(fileId);
      out.writeLong(position);
    }

    @Override
    public void close(TaskAttemptContext context) throws IOException {
      out.close();
      dictOut.close();
    }
  }

  protected static class VariableLengthRecordWriter
      extends FixedLengthRecordWriter {

    private static final int BLK_SIZE = 8192;

    protected VariableLengthRecordWriter(
        DataOutputStream out, DataOutputStream dictOut, JobContext context) {
      super(out, dictOut, context);
    }

    @Override
    protected void writeRecord(long id, int fileId, long position)
        throws IOException {
      int freeBytesInBlock =
        BLK_SIZE - (int)(((FSDataOutputStream) out).getPos() & (BLK_SIZE - 1));
      if (freeBytesInBlock < 23) {
        for (int i = 0; i < freeBytesInBlock; ++i) {
          out.writeByte(0xad);
        }
      }
      WritableUtils.writeVLong(out, id);
      WritableUtils.writeVInt(out, fileId);
      WritableUtils.writeVLong(out, position);
    }
  }

  public static boolean getCompactOutput(JobContext job) {
    return job.getConfiguration().getBoolean(COMPACT_OUTPUT, true);
  }

  public static void setCompactOutput(JobContext job, boolean compact) {
    job.getConfiguration().setBoolean(COMPACT_OUTPUT, compact);
  }

  @Override
  public RecordWriter<LongWritable, FilePosition>
      getRecordWriter(TaskAttemptContext job)
      throws IOException, InterruptedException {
    Configuration conf = job.getConfiguration();
    Path file = getDefaultWorkFile(job, "");
    FileOutputFormat.setOutputName(job, RAMP_DICT);
    Path dictFile = getDefaultWorkFile(job, "");

    FileSystem fs = file.getFileSystem(conf);
    FSDataOutputStream fileOut = fs.create(file, false);
    FSDataOutputStream dictFileOut = fs.create(dictFile, false);

    if (getCompactOutput(job)) {
      return new VariableLengthRecordWriter(fileOut, dictFileOut, job);
    } else {
      return new FixedLengthRecordWriter(fileOut, dictFileOut, job);
    }
  }

}
