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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configuration.IntegerRanges;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.MapContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.security.Credentials;

/**
 * MapContext for the wrapped mapper class.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
class RampMapContextImpl<KEYIN, VALUEIN extends Writable,
    KEYOUT, VALUEOUT extends Writable, PROVENANCE extends Writable>
    implements MapContext<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {

  private final MapContext<KEYIN, RampPair<VALUEIN, PROVENANCE>,
      KEYOUT, RampPair<VALUEOUT, PROVENANCE>> baseMapContext;
  private final Configuration conf;
  private final RampPair<VALUEOUT, PROVENANCE> valueProvenancePair =
    new RampPair<VALUEOUT, PROVENANCE>();

  RampMapContextImpl(
      MapContext<KEYIN, RampPair<VALUEIN, PROVENANCE>,
          KEYOUT, RampPair<VALUEOUT, PROVENANCE>> baseMapContext,
      Configuration conf) {
    this.baseMapContext = baseMapContext;
    this.conf = conf;
  }

  @Override
  public InputSplit getInputSplit() {
    return baseMapContext.getInputSplit();
  }

  @Override
  public KEYIN getCurrentKey() throws IOException, InterruptedException {
    return baseMapContext.getCurrentKey();
  }

  @Override
  public VALUEIN getCurrentValue() throws IOException, InterruptedException {
    return baseMapContext.getCurrentValue().getValue();
  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    return baseMapContext.nextKeyValue();
  }

  @Override
  public Counter getCounter(Enum<?> counterName) {
    return baseMapContext.getCounter(counterName);
  }

  @Override
  public Counter getCounter(String groupName, String counterName) {
    return baseMapContext.getCounter(groupName, counterName);
  }

  @Override
  public OutputCommitter getOutputCommitter() {
    return baseMapContext.getOutputCommitter();
  }

  @Override
  public void write(KEYOUT key, VALUEOUT value)
      throws IOException, InterruptedException {
    valueProvenancePair.setValue(value);
    valueProvenancePair.setProvenance(
        baseMapContext.getCurrentValue().getProvenance());
    baseMapContext.write(key, valueProvenancePair);
  }

  @Override
  public String getStatus() {
    return baseMapContext.getStatus();
  }

  @Override
  public TaskAttemptID getTaskAttemptID() {
    return baseMapContext.getTaskAttemptID();
  }

  @Override
  public void setStatus(String msg) {
    baseMapContext.setStatus(msg);
  }

  @Override
  public Path[] getArchiveClassPaths() {
    return baseMapContext.getArchiveClassPaths();
  }

  @Override
  public String[] getArchiveTimestamps() {
    return baseMapContext.getArchiveTimestamps();
  }

  @Override
  public URI[] getCacheArchives() throws IOException {
    return baseMapContext.getCacheArchives();
  }

  @Override
  public URI[] getCacheFiles() throws IOException {
    return baseMapContext.getCacheFiles();
  }

  @Override
  public Class<? extends Reducer<?, ?, ?, ?>> getCombinerClass()
      throws ClassNotFoundException {
    return baseMapContext.getCombinerClass();
  }

  @Override
  public Configuration getConfiguration() {
    return conf;
  }

  @Override
  public Path[] getFileClassPaths() {
    return baseMapContext.getFileClassPaths();
  }

  @Override
  public String[] getFileTimestamps() {
    return baseMapContext.getFileTimestamps();
  }

  @Override
  public RawComparator<?> getGroupingComparator() {
    return baseMapContext.getGroupingComparator();
  }

  @Override
  public Class<? extends InputFormat<?, ?>> getInputFormatClass()
      throws ClassNotFoundException {
    return baseMapContext.getInputFormatClass();
  }

  @Override
  public String getJar() {
    return baseMapContext.getJar();
  }

  @Override
  public JobID getJobID() {
    return baseMapContext.getJobID();
  }

  @Override
  public String getJobName() {
    return baseMapContext.getJobName();
  }

  @Override
  public boolean getJobSetupCleanupNeeded() {
    return baseMapContext.getJobSetupCleanupNeeded();
  }

  @Override
  public boolean getTaskCleanupNeeded() {
    return baseMapContext.getTaskCleanupNeeded();
  }

  @Override
  public Path[] getLocalCacheArchives() throws IOException {
    return baseMapContext.getLocalCacheArchives();
  }

  @Override
  public Path[] getLocalCacheFiles() throws IOException {
    return baseMapContext.getLocalCacheFiles();
  }

  @Override
  public Class<?> getMapOutputKeyClass() {
    return baseMapContext.getMapOutputKeyClass();
  }

  @Override
  public Class<?> getMapOutputValueClass() {
    return baseMapContext.getMapOutputValueClass();
  }

  @Override
  public Class<? extends Mapper<?, ?, ?, ?>> getMapperClass()
      throws ClassNotFoundException {
    return baseMapContext.getMapperClass();
  }

  @Override
  public int getMaxMapAttempts() {
    return baseMapContext.getMaxMapAttempts();
  }

  @Override
  public int getMaxReduceAttempts() {
    return baseMapContext.getMaxReduceAttempts();
  }

  @Override
  public int getNumReduceTasks() {
    return baseMapContext.getNumReduceTasks();
  }

  @Override
  public Class<? extends OutputFormat<?, ?>> getOutputFormatClass()
      throws ClassNotFoundException {
    return baseMapContext.getOutputFormatClass();
  }

  @Override
  public Class<?> getOutputKeyClass() {
    return baseMapContext.getOutputKeyClass();
  }

  @Override
  public Class<?> getOutputValueClass() {
    return baseMapContext.getOutputValueClass();
  }

  @Override
  public Class<? extends Partitioner<?, ?>> getPartitionerClass()
      throws ClassNotFoundException {
    return baseMapContext.getPartitionerClass();
  }

  @Override
  public boolean getProfileEnabled() {
    return baseMapContext.getProfileEnabled();
  }

  @Override
  public String getProfileParams() {
    return baseMapContext.getProfileParams();
  }

  @Override
  public IntegerRanges getProfileTaskRange(boolean isMap) {
    return baseMapContext.getProfileTaskRange(isMap);
  }

  @Override
  public Class<? extends Reducer<?, ?, ?, ?>> getReducerClass()
      throws ClassNotFoundException {
    return baseMapContext.getReducerClass();
  }

  @Override
  public RawComparator<?> getSortComparator() {
    return baseMapContext.getSortComparator();
  }

  @Override
  public boolean getSymlink() {
    return baseMapContext.getSymlink();
  }

  @Override
  public String getUser() {
    return baseMapContext.getUser();
  }

  @Override
  public Path getWorkingDirectory() throws IOException {
    return baseMapContext.getWorkingDirectory();
  }

  @Override
  public void progress() {
    baseMapContext.progress();
  }

  @Override
  public Credentials getCredentials() {
    return baseMapContext.getCredentials();
  }

  @Override
  public float getProgress() {
    return baseMapContext.getProgress();
  }
}
