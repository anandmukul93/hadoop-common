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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * The RampPartitioner class
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
class RampPartitioner<KEY, VALUE extends Writable, PROVENANCE extends Writable>
    extends Partitioner<KEY, RampPair<VALUE, PROVENANCE>>
    implements Configurable {

  private Partitioner<KEY, VALUE> userPartitioner = null;
  private Configuration conf = null;

  @Override
  public int getPartition(KEY key, RampPair<VALUE, PROVENANCE> value,
      int numPartitions) {
    return userPartitioner.getPartition(key, value.getValue(), numPartitions);
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  @SuppressWarnings("unchecked")
  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;

    userPartitioner = ReflectionUtils.newInstance(
        conf.getClass(Ramp.PARTITIONER_CLASS, null, Partitioner.class),
        conf);
  }

}
