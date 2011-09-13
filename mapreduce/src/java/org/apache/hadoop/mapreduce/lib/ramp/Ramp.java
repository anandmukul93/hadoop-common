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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.ramp.file.FilePosition;
import org.apache.hadoop.mapreduce.lib.ramp.file.MapProvenanceFormat;
import org.apache.hadoop.mapreduce.lib.ramp.file.ReduceProvenanceFormat;

/**
 * The Ramp class
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class Ramp {

  protected static final String MAPPER_CLASS =
    "mapreduce.ramp.mapper.class";
  protected static final String REDUCER_CLASS =
    "mapreduce.ramp.reducer.class";
  protected static final String COMBINER_CLASS =
    "mapreduce.ramp.combiner.class";
  protected static final String PARTITIONER_CLASS =
    "mapreduce.ramp.partitioner.class";
  protected static final String INPUT_FORMAT_CLASS =
    "mapreduce.ramp.job.inputformat.class";
  protected static final String OUTPUT_FORMAT_CLASS =
    "mapreduce.ramp.job.outputformat.class";

  protected static final String MAP_OUTPUT_KEY_CLASS =
    "mapreduce.ramp.map.output.key.class";
  protected static final String MAP_OUTPUT_VALUE_CLASS =
    "mapreduce.ramp.map.output.value.class";
  protected static final String OUTPUT_KEY_CLASS =
    "mapreduce.ramp.job.output.key.class";
  protected static final String OUTPUT_VALUE_CLASS =
    "mapreduce.ramp.job.output.value.class";

  protected static final String PROVENANCE_CLASS =
    "mapreduce.ramp.job.provenance.class";
  protected static final String MAP_PROVENANCE_OUTPUT_FORMAT_CLASS =
    "mapreduce.ramp.map.provenance.outputformat.class";
  protected static final String PROVENANCE_OUTPUT_FORMAT_CLASS =
    "mapreduce.ramp.job.provenance.outputformat.class";

  public static void setProvenanceCapture(Job job)
      throws ClassNotFoundException {
    Configuration jobConf = job.getConfiguration();

    jobConf.setClass(MAPPER_CLASS, job.getMapperClass(), Mapper.class);
    jobConf.setClass(REDUCER_CLASS, job.getReducerClass(), Reducer.class);
    if (job.getCombinerClass() != null) {
      jobConf.setClass(COMBINER_CLASS, job.getCombinerClass(), Reducer.class);
    }
    jobConf.setClass(PARTITIONER_CLASS,
        job.getPartitionerClass(), Partitioner.class);
    jobConf.setClass(INPUT_FORMAT_CLASS,
        job.getInputFormatClass(), InputFormat.class);
    jobConf.setClass(OUTPUT_FORMAT_CLASS,
        job.getOutputFormatClass(), OutputFormat.class);

    jobConf.setClass(MAP_OUTPUT_KEY_CLASS,
        job.getMapOutputKeyClass(), Object.class);
    jobConf.setClass(MAP_OUTPUT_VALUE_CLASS,
        job.getMapOutputValueClass(), Writable.class);
    jobConf.setClass(OUTPUT_KEY_CLASS, job.getOutputKeyClass(), Object.class);
    jobConf.setClass(OUTPUT_VALUE_CLASS,
        job.getOutputValueClass(), Writable.class);

    job.setMapperClass(RampMapper.class);
    job.setReducerClass(RampReducer.class);
    if (job.getCombinerClass() != null) {
      job.setCombinerClass(RampCombiner.class);
    }
    job.setPartitionerClass(RampPartitioner.class);
    job.setInputFormatClass(RampInputFormat.class);
    job.setOutputFormatClass(RampOutputFormat.class);

    job.setMapOutputValueClass(RampPair.class);
    job.setOutputValueClass(RampPair.class);
  }

  @SuppressWarnings("deprecation")
  public static void setProvenanceCapture(
      org.apache.hadoop.mapred.jobcontrol.Job oldJob)
      throws ClassNotFoundException, IOException {
    Configuration jobConf = oldJob.getJobConf();
    Job job = new Job(jobConf);
    setProvenanceCapture(job);
    oldJob.setJobConf((JobConf) job.getConfiguration());
  }

  public static void setProvenanceClass(
      Job job, Class<? extends Writable> cls) {
    job.getConfiguration().setClass(PROVENANCE_CLASS, cls, Writable.class);
  }

  @SuppressWarnings("unchecked")
  public static void setMapProvenanceOutputFormatClass(
      Job job, Class<? extends OutputFormat> cls) {
    job.getConfiguration().setClass(MAP_PROVENANCE_OUTPUT_FORMAT_CLASS,
        cls, OutputFormat.class);
  }

  @SuppressWarnings("unchecked")
  public static void setProvenanceOutputFormatClass(
      Job job, Class<? extends OutputFormat> cls) {
    job.getConfiguration().setClass(PROVENANCE_OUTPUT_FORMAT_CLASS,
        cls, OutputFormat.class);
  }

  public static Class<?> getProvenanceClass(Configuration conf) {
    return conf.getClass(PROVENANCE_CLASS, FilePosition.class);
  }

  public static Class<?> getMapProvenanceOutputFormatClass(
      Configuration conf) {
    return conf.getClass(Ramp.MAP_PROVENANCE_OUTPUT_FORMAT_CLASS,
        MapProvenanceFormat.class);
  }

  public static Class<?> getProvenanceOutputFormatClass(
      Configuration conf, int numReduceTasks) {
    return conf.getClass(Ramp.PROVENANCE_OUTPUT_FORMAT_CLASS,
        (numReduceTasks == 0) ?
            MapProvenanceFormat.class :
            ReduceProvenanceFormat.class);
  }

}
