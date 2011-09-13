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
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * The RampInputFormat class
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
class RampInputFormat<KEYIN, VALUEIN extends Writable,
    PROVENANCE extends Writable>
    extends InputFormat<KEYIN, RampPair<VALUEIN, PROVENANCE>>
    implements Configurable {

  private InputFormat<KEYIN, VALUEIN> userInputFormat = null;
  private Configuration conf = null;

  private static class RampRecordReader<KEYIN, VALUEIN extends Writable,
      PROVENANCE extends Writable>
      extends RecordReader<KEYIN, RampPair<VALUEIN, PROVENANCE>> {

    private final RecordReader<KEYIN, VALUEIN> userRecordReader;
    private final RampPair<VALUEIN, PROVENANCE> valueProvenancePair =
      new RampPair<VALUEIN, PROVENANCE>();

    private RampRecordReader(RecordReader<KEYIN, VALUEIN> userRecordReader) {
      this.userRecordReader = userRecordReader;
    }

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context)
        throws IOException, InterruptedException {
      userRecordReader.initialize(split, context);
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
      return userRecordReader.nextKeyValue();
    }

    @Override
    public KEYIN getCurrentKey() throws IOException, InterruptedException {
      return userRecordReader.getCurrentKey();
    }

    @SuppressWarnings("unchecked")
    @Override
    public RampPair<VALUEIN, PROVENANCE> getCurrentValue()
        throws IOException, InterruptedException {
      valueProvenancePair.setValue(userRecordReader.getCurrentValue());
      valueProvenancePair.setProvenance(
          (PROVENANCE) userRecordReader.getCurrentID());
      return valueProvenancePair;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
      return userRecordReader.getProgress();
    }

    @Override
    public void close() throws IOException {
      userRecordReader.close();
    }
  }


  @Override
  public List<InputSplit> getSplits(JobContext context)
      throws IOException, InterruptedException {
    return userInputFormat.getSplits(context);
  }

  @Override
  public RecordReader<KEYIN, RampPair<VALUEIN, PROVENANCE>>
      createRecordReader(InputSplit split, TaskAttemptContext context)
      throws IOException, InterruptedException {
    return new RampRecordReader<KEYIN, VALUEIN, PROVENANCE>(
        userInputFormat.createRecordReader(split, context));
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  @SuppressWarnings("unchecked")
  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;

    userInputFormat = ReflectionUtils.newInstance(
        conf.getClass(Ramp.INPUT_FORMAT_CLASS, null, InputFormat.class),
        conf);
  }

}
