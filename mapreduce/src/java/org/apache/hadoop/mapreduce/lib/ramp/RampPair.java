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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.util.ReflectionUtils;

@InterfaceAudience.Private
@InterfaceStability.Unstable
class RampPair<VALUE extends Writable, PROVENANCE extends Writable>
    implements Writable, Configurable {

  private VALUE value;
  private List<PROVENANCE> provenance;
  private Configuration conf = null;

  RampPair() {
    this.value = null;
    this.provenance = new LinkedList<PROVENANCE>();
  }

  RampPair(VALUE value, PROVENANCE provenance) {
    this.value = value;
    this.provenance = new LinkedList<PROVENANCE>();
    this.provenance.add(provenance);
  }

  RampPair(VALUE value, List<PROVENANCE> provenance) {
    this.value = value;
    this.provenance = provenance;
  }

  public void setValue(VALUE value) {
    this.value = value;
  }

  public void setProvenance(PROVENANCE provenance) {
    if (this.provenance.isEmpty()) {
      this.provenance.add(provenance);
    } else {
      this.provenance.set(0, provenance);
    }
  }

  public void setProvenance(List<PROVENANCE> provenance) {
    this.provenance = provenance;
  }

  public VALUE getValue() {
    return value;
  }

  public List<PROVENANCE> getProvenance() {
    return provenance;
  }

  @SuppressWarnings("unchecked")
  @Override
  public void readFields(DataInput in) throws IOException {
    if (value == null) {
      value = (VALUE) ReflectionUtils.newInstance(
          conf.getClass(Ramp.MAP_OUTPUT_VALUE_CLASS, null, Writable.class),
          conf);
    }
    value.readFields(in);

    int size = WritableUtils.readVInt(in);
    provenance.clear();
    for (int i = 0; i < size; ++i) {
      PROVENANCE prov = (PROVENANCE) ReflectionUtils.newInstance(
          Ramp.getProvenanceClass(conf), conf);
      prov.readFields(in);
      provenance.add(prov);
    }
  }

  @Override
  public void write(DataOutput out) throws IOException {
    value.write(out);

    WritableUtils.writeVInt(out, provenance.size());
    for (PROVENANCE prov: provenance) {
      prov.write(out);
    }
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

}
