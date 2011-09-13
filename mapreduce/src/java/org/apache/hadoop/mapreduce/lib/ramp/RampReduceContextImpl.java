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
import java.net.URI;
import java.util.Iterator;
import java.util.NoSuchElementException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configuration.IntegerRanges;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.ReduceContext;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskCounter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * ReduceContext for the wrapped reducer class.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
class RampReduceContextImpl<KEYIN extends Writable, VALUEIN extends Writable,
    KEYOUT, VALUEOUT extends Writable, PROVENANCE extends Writable>
    implements ReduceContext<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {

  protected static final String RAMP_MAP = "_ramp_map";

  protected final ReduceContext<KEYIN, RampPair<VALUEIN, PROVENANCE>,
      KEYOUT, RampPair<VALUEOUT, ?>> baseReduceContext;
  private final Configuration conf;
  private final Counter inputKeyCounter;
  private final ValueIterable iterable = new ValueIterable();
  protected boolean firstValue = false;

  private LongWritable provenanceId = null;
  private RampPair<VALUEOUT, LongWritable> valueProvenancePair = null;
  private RecordWriter<LongWritable, PROVENANCE> mapProvenanceWriter = null;

  RampReduceContextImpl(
      ReduceContext<KEYIN, RampPair<VALUEIN, PROVENANCE>,
          KEYOUT, RampPair<VALUEOUT, ?>> baseReduceContext,
      Configuration conf, boolean isCombineContext)
      throws IOException, InterruptedException {
    this.baseReduceContext = baseReduceContext;
    this.conf = conf;
    this.inputKeyCounter = getCounter(TaskCounter.REDUCE_INPUT_GROUPS);

    if (!isCombineContext) {
      provenanceId = new LongWritable(0);
      valueProvenancePair =
        new RampPair<VALUEOUT, LongWritable>(null, provenanceId);
      mapProvenanceWriter = getRecordWriter(baseReduceContext);
    }
  }

  @SuppressWarnings("unchecked")
  private RecordWriter<LongWritable, PROVENANCE>
      getRecordWriter(TaskAttemptContext job)
      throws IOException, InterruptedException {
    TaskAttemptContext provContext =
      new TaskAttemptContextImpl(
          job.getConfiguration(), job.getTaskAttemptID());
    FileOutputFormat.setOutputName(provContext, RAMP_MAP);
    ((JobConf) provContext.getConfiguration()).setOutputKeyClass(
        LongWritable.class);
    ((JobConf) provContext.getConfiguration()).setOutputValueClass(
        Ramp.getProvenanceClass(conf));

    return ((OutputFormat<LongWritable, PROVENANCE>) ReflectionUtils.newInstance(
        Ramp.getMapProvenanceOutputFormatClass(conf),
        provContext.getConfiguration())).getRecordWriter(provContext);
  }

  public void close(TaskAttemptContext context)
      throws IOException, InterruptedException {
    mapProvenanceWriter.close(context);
  }

  @Override
  public boolean nextKey() throws IOException, InterruptedException {
    while (nextKeyIsSame()) {
      if (!nextKeyValue()) {
        throw new RuntimeException("nextKeyValue() returned false");
      }
    }

    provenanceId.set(provenanceId.get() + 1);
    if (nextKeyValue()) {
      if (inputKeyCounter != null) {
        inputKeyCounter.increment(1);
      }
      return true;
    } else {
      return false;
    }
  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    firstValue = !nextKeyIsSame();

    if (baseReduceContext.nextKeyValue()) {
      for (PROVENANCE provenance:
          baseReduceContext.getCurrentValue().getProvenance()) {
        mapProvenanceWriter.write(provenanceId, provenance);
      }
      return true;
    } else {
      return false;
    }
  }

  @Override
  public KEYIN getCurrentKey() throws IOException, InterruptedException {
    return baseReduceContext.getCurrentKey();
  }

  @Override
  public VALUEIN getCurrentValue() throws IOException, InterruptedException {
    return baseReduceContext.getCurrentValue().getValue();
  }

  protected class ValueIterator
      implements ReduceContext.ValueIterator<VALUEIN> {

    @Override
    public boolean hasNext() {
      return firstValue || nextKeyIsSame();
    }

    @Override
    public VALUEIN next() {
      // if this is the first record, we don't need to advance
      if (firstValue) {
        firstValue = false;
        try {
          return getCurrentValue();
        } catch (IOException e) {
          throw new RuntimeException("getCurrentValue() failed", e);
        } catch (InterruptedException e) {
          throw new RuntimeException("getCurrentValue() failed", e);
        }
      }
      // if this isn't the first record and the next key is different, they
      // can't advance it here.
      if (!nextKeyIsSame()) {
        throw new NoSuchElementException("iterate past last value");
      }
      // otherwise, go to the next key/value pair
      try {
        nextKeyValue();
        return getCurrentValue();
      } catch (IOException ie) {
        throw new RuntimeException("next value iterator failed", ie);
      } catch (InterruptedException ie) {
        // this is bad, but we can't modify the exception list of java.util
        throw new RuntimeException("next value iterator interrupted", ie);
      }
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException("remove not implemented");
    }

    @Override
    public void mark() throws IOException {
      throw new UnsupportedOperationException("mark not implemented");
    }

    @Override
    public void reset() throws IOException {
      throw new UnsupportedOperationException("reset not implemented");
    }

    @Override
    public void clearMark() throws IOException {
      throw new UnsupportedOperationException("clearMark not implemented");
    }

    @Override
    public void resetBackupStore() throws IOException {
    }
  }

  protected class ValueIterable implements Iterable<VALUEIN> {
    private final ValueIterator iterator = new ValueIterator();
    @Override
    public Iterator<VALUEIN> iterator() {
      return iterator;
    }
  }

  @Override
  public
  Iterable<VALUEIN> getValues() throws IOException, InterruptedException {
    return iterable;
  }

  @Override
  public Counter getCounter(Enum<?> counterName) {
    return baseReduceContext.getCounter(counterName);
  }

  @Override
  public Counter getCounter(String groupName, String counterName) {
    return baseReduceContext.getCounter(groupName, counterName);
  }

  @Override
  public OutputCommitter getOutputCommitter() {
    return baseReduceContext.getOutputCommitter();
  }

  @Override
  public void write(KEYOUT key, VALUEOUT value)
      throws IOException, InterruptedException {
    valueProvenancePair.setValue(value);
    baseReduceContext.write(key, valueProvenancePair);
  }

  @Override
  public String getStatus() {
    return baseReduceContext.getStatus();
  }

  @Override
  public TaskAttemptID getTaskAttemptID() {
    return baseReduceContext.getTaskAttemptID();
  }

  @Override
  public void setStatus(String msg) {
    baseReduceContext.setStatus(msg);
  }

  @Override
  public Path[] getArchiveClassPaths() {
    return baseReduceContext.getArchiveClassPaths();
  }

  @Override
  public long[] getArchiveTimestamps() {
    return baseReduceContext.getArchiveTimestamps();
  }

  @Override
  public URI[] getCacheArchives() throws IOException {
    return baseReduceContext.getCacheArchives();
  }

  @Override
  public URI[] getCacheFiles() throws IOException {
    return baseReduceContext.getCacheFiles();
  }

  @Override
  public Class<? extends Reducer<?, ?, ?, ?>> getCombinerClass()
      throws ClassNotFoundException {
    return baseReduceContext.getCombinerClass();
  }

  @Override
  public Configuration getConfiguration() {
    return conf;
  }

  @Override
  public Path[] getFileClassPaths() {
    return baseReduceContext.getFileClassPaths();
  }

  @Override
  public long[] getFileTimestamps() {
    return baseReduceContext.getFileTimestamps();
  }

  @Override
  public RawComparator<?> getGroupingComparator() {
    return baseReduceContext.getGroupingComparator();
  }

  @Override
  public Class<? extends InputFormat<?, ?>> getInputFormatClass()
      throws ClassNotFoundException {
    return baseReduceContext.getInputFormatClass();
  }

  @Override
  public String getJar() {
    return baseReduceContext.getJar();
  }

  @Override
  public JobID getJobID() {
    return baseReduceContext.getJobID();
  }

  @Override
  public String getJobName() {
    return baseReduceContext.getJobName();
  }

  @Override
  public boolean getJobSetupCleanupNeeded() {
    return baseReduceContext.getJobSetupCleanupNeeded();
  }

  @Override
  public Path[] getLocalCacheArchives() throws IOException {
    return baseReduceContext.getLocalCacheArchives();
  }

  @Override
  public Path[] getLocalCacheFiles() throws IOException {
    return baseReduceContext.getLocalCacheFiles();
  }

  @Override
  public Class<?> getMapOutputKeyClass() {
    return baseReduceContext.getMapOutputKeyClass();
  }

  @Override
  public Class<?> getMapOutputValueClass() {
    return baseReduceContext.getMapOutputValueClass();
  }

  @Override
  public Class<? extends Mapper<?, ?, ?, ?>> getMapperClass()
      throws ClassNotFoundException {
    return baseReduceContext.getMapperClass();
  }

  @Override
  public int getMaxMapAttempts() {
    return baseReduceContext.getMaxMapAttempts();
  }

  @Override
  public int getMaxReduceAttempts() {
    return baseReduceContext.getMaxReduceAttempts();
  }

  @Override
  public int getNumReduceTasks() {
    return baseReduceContext.getNumReduceTasks();
  }

  @Override
  public Class<? extends OutputFormat<?, ?>> getOutputFormatClass()
      throws ClassNotFoundException {
    return baseReduceContext.getOutputFormatClass();
  }

  @Override
  public Class<?> getOutputKeyClass() {
    return baseReduceContext.getOutputKeyClass();
  }

  @Override
  public Class<?> getOutputValueClass() {
    return baseReduceContext.getOutputValueClass();
  }

  @Override
  public Class<? extends Partitioner<?, ?>> getPartitionerClass()
      throws ClassNotFoundException {
    return baseReduceContext.getPartitionerClass();
  }

  @Override
  public Class<? extends Reducer<?, ?, ?, ?>> getReducerClass()
      throws ClassNotFoundException {
    return baseReduceContext.getReducerClass();
  }

  @Override
  public RawComparator<?> getSortComparator() {
    return baseReduceContext.getSortComparator();
  }

  @Override
  public boolean getSymlink() {
    return baseReduceContext.getSymlink();
  }

  @Override
  public Path getWorkingDirectory() throws IOException {
    return baseReduceContext.getWorkingDirectory();
  }

  @Override
  public void progress() {
    baseReduceContext.progress();
  }

  @Override
  public boolean getProfileEnabled() {
    return baseReduceContext.getProfileEnabled();
  }

  @Override
  public String getProfileParams() {
    return baseReduceContext.getProfileParams();
  }

  @Override
  public IntegerRanges getProfileTaskRange(boolean isMap) {
    return baseReduceContext.getProfileTaskRange(isMap);
  }

  @Override
  public String getUser() {
    return baseReduceContext.getUser();
  }

  @Override
  public boolean nextKeyIsSame() {
    return baseReduceContext.nextKeyIsSame();
  }

  @Override
  public Credentials getCredentials() {
    return baseReduceContext.getCredentials();
  }

}
