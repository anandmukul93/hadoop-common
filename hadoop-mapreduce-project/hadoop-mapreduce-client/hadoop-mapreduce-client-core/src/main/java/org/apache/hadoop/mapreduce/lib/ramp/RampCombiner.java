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
import org.apache.hadoop.mapreduce.ReduceContext;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.reduce.WrappedReducer;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * The RampCombiner class
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
class RampCombiner<KEY extends Writable, VALUE extends Writable,
    PROVENANCE extends Writable>
    extends Reducer<KEY, RampPair<VALUE, PROVENANCE>,
        KEY, RampPair<VALUE, ?>> {

  @SuppressWarnings("unchecked")
  @Override
  public void run(Context context) throws IOException, InterruptedException {
    Configuration jobConf = context.getConfiguration();
    Reducer<KEY, VALUE, KEY, VALUE> userCombiner =
      ReflectionUtils.newInstance(
          jobConf.getClass(Ramp.COMBINER_CLASS, null, Reducer.class), jobConf);

    ReduceContext<KEY, VALUE, KEY, VALUE> combineContext =
      new RampCombineContextImpl<KEY, VALUE, PROVENANCE>(
          context, jobConf);
    Reducer<KEY, VALUE, KEY, VALUE>.Context reducerContext =
      new WrappedReducer<KEY, VALUE, KEY, VALUE>().getReducerContext(
          combineContext);

    userCombiner.run(reducerContext);
  }

}
