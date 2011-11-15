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
package org.apache.hadoop.mapreduce.lib.ramp;

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * The RampOutputFormat class
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
class RampOutputFormat<KEYOUT, VALUEOUT extends Writable,
    PROVENANCE extends Writable>
    extends OutputFormat<KEYOUT, RampPair<VALUEOUT, PROVENANCE>>
    implements Configurable {

  private static final String RAMP = "_ramp";

  private OutputFormat<KEYOUT, VALUEOUT> userOutputFormat = null;
  private Configuration conf = null;

  private static class RampRecordWriter<KEYOUT, VALUEOUT extends Writable,
      PROVENANCE extends Writable>
      extends RecordWriter<KEYOUT, RampPair<VALUEOUT, PROVENANCE>> {

    private final RecordWriter<KEYOUT, VALUEOUT> userRecordWriter;
    private final RecordWriter<Writable, PROVENANCE> provenanceWriter;

    private RampRecordWriter(RecordWriter<KEYOUT, VALUEOUT> userRecordWriter,
        RecordWriter<Writable, PROVENANCE> provenanceWriter) {
      this.userRecordWriter = userRecordWriter;
      this.provenanceWriter = provenanceWriter;
    }

    public void write(KEYOUT key,
        RampPair<VALUEOUT, PROVENANCE> value)
        throws IOException, InterruptedException {
      userRecordWriter.write(key, value.getValue());
      provenanceWriter.write(
          userRecordWriter.getCurrentID(),
          value.getProvenance().get(0));
    }

    public void close(TaskAttemptContext context)
        throws IOException, InterruptedException {
      userRecordWriter.close(context);
      provenanceWriter.close(context);
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public RecordWriter<KEYOUT, RampPair<VALUEOUT, PROVENANCE>>
      getRecordWriter(TaskAttemptContext job)
      throws IOException, InterruptedException {
    TaskAttemptContext userContext =
      new TaskAttemptContextImpl(
          job.getConfiguration(), job.getTaskAttemptID());
    ((JobConf) userContext.getConfiguration()).setOutputValueClass(
        conf.getClass(Ramp.OUTPUT_VALUE_CLASS, null));

    TaskAttemptContext provContext =
      new TaskAttemptContextImpl(
          job.getConfiguration(), job.getTaskAttemptID());
    FileOutputFormat.setOutputName(provContext, RAMP);
    ((JobConf) provContext.getConfiguration()).setOutputValueClass(
        conf.getClass(Ramp.MAP_OUTPUT_KEY_CLASS, null));
    RecordWriter<Writable, PROVENANCE> provenanceWriter =
      ((OutputFormat<Writable, PROVENANCE>) ReflectionUtils.newInstance(
          Ramp.getProvenanceOutputFormatClass(conf, job.getNumReduceTasks()),
          provContext.getConfiguration())).getRecordWriter(provContext);

    return new RampRecordWriter<KEYOUT, VALUEOUT, PROVENANCE>(
        userOutputFormat.getRecordWriter(userContext), provenanceWriter);
  }

  @Override
  public void checkOutputSpecs(JobContext context)
      throws IOException, InterruptedException {
    userOutputFormat.checkOutputSpecs(context);
  }

  @Override
  public OutputCommitter getOutputCommitter(TaskAttemptContext context)
      throws IOException, InterruptedException {
    return userOutputFormat.getOutputCommitter(context);
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  @SuppressWarnings("unchecked")
  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;

    userOutputFormat = ReflectionUtils.newInstance(
        conf.getClass(Ramp.OUTPUT_FORMAT_CLASS, null, OutputFormat.class),
        conf);
  }

}
