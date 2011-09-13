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
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.ReduceContext;

/**
 * CombineContext for the wrapped combiner class.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
class RampCombineContextImpl<KEY extends Writable, VALUE extends Writable,
    PROVENANCE extends Writable>
    extends RampReduceContextImpl<KEY, VALUE, KEY, VALUE, PROVENANCE> {

  private final List<PROVENANCE> provenances = new LinkedList<PROVENANCE>();
  private final RampPair<VALUE, PROVENANCE> valueProvenancePair =
    new RampPair<VALUE, PROVENANCE>(null, provenances);
  private KEY key;
  private VALUE value;

  RampCombineContextImpl(
      ReduceContext<KEY, RampPair<VALUE, PROVENANCE>,
          KEY, RampPair<VALUE, ?>> baseReduceContext,
      Configuration conf) throws IOException, InterruptedException {
    super(baseReduceContext, conf, true);
  }

  @Override
  public boolean nextKey() throws IOException, InterruptedException {
    while (nextKeyIsSame()) {
      if (!nextKeyValue()) {
        throw new RuntimeException("nextKeyValue() returned false");
      }
    }

    if (key != null || value != null) {
      valueProvenancePair.setValue(value);
      baseReduceContext.write(key, valueProvenancePair);
      key = null;
      value = null;
    }
    provenances.clear();
    return nextKeyValue();
  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    firstValue = !nextKeyIsSame();

    if (baseReduceContext.nextKeyValue()) {
      provenances.addAll(
          baseReduceContext.getCurrentValue().getProvenance());
      return true;
    } else {
      return false;
    }
  }

  @Override
  public void write(KEY key, VALUE value)
      throws IOException, InterruptedException {
    if (nextKeyIsSame()) {
      if (this.key != null || this.value != null) {
        throw new RuntimeException("write() called twice");
      }
      this.key = key;
      this.value = value;
    } else {
      valueProvenancePair.setValue(value);
      baseReduceContext.write(key, valueProvenancePair);
    }
  }

}
