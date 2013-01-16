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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

/**
 * The FilePosition class
 */
public class FilePosition implements Writable {

  private int dir;
  private Text file;
  private long position;

  public FilePosition() {
    file = new Text();
    position = -1;
    dir = -1;
  }

  public FilePosition(Path file, Path[] dirs) {
    for (dir = 0; dir < dirs.length; ++dir) {
      if (file.toString().startsWith(dirs[dir].toString() + "/")) {
        break;
      }
    }

    if (dir < dirs.length) {
      this.file =
        new Text(dirs[dir].toUri().relativize(file.toUri()).toString());
    } else {
      this.file = new Text(file.toString());
      dir = -1;
    }
    position = -1;
  }

  public FilePosition(String file, long position) {
    dir = -1;
    this.file = new Text(file);
    this.position = position;
  }

  public void setPosition(long position) {
    this.position = position;
  }

  public int getDir() {
    return dir;
  }

  public Text getFile() {
    return file;
  }

  public long getPosition() {
    return position;
  }

  @Override
  public String toString() {
    return file.toString() + "@" + position;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    WritableUtils.writeVInt(out, dir);
    file.write(out);
    WritableUtils.writeVLong(out, position);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    dir = WritableUtils.readVInt(in);
    file.readFields(in);
    position = WritableUtils.readVLong(in);
  }

}
