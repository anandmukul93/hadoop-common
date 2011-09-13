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
package org.apache.hadoop.hdfs.server.common;

import java.io.EOFException;
import java.io.IOException;
import java.util.HashMap;
import java.util.StringTokenizer;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.jsp.JspWriter;

import org.apache.commons.lang.StringEscapeUtils;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.util.LineReader;

@InterfaceAudience.Private
public class RampJspHelper {

  private static final int BLK_SIZE = 8192;

  private static HashMap<Path, HashMap<Integer, String>> dictCache =
    new HashMap<Path, HashMap<Integer, String>>();
  private static HashMap<Path, FSDataInputStream> streamCache =
    new HashMap<Path, FSDataInputStream>();
  private static HashMap<Path, Long> fileSizeCache = new HashMap<Path, Long>();
  private static FileSystem fs;
  private static JspWriter jspOut;

  public static void generateProvenance(JspWriter out, HttpServletRequest req,
      Configuration conf)
      throws IOException, InterruptedException {
    String filename = JspHelper.validatePath(req.getParameter("filename"));
    if (filename == null) {
      out.print("Invalid input (filename absent)");
      return;
    }

    Long offset = JspHelper.validateLong(req.getParameter("offset"));
    if (offset == null) {
      out.print("Invalid input (offset absent)");
      return;
    }

    jspOut = out;
    fs = new Path(filename).getFileSystem(conf);

    out.println("<div><pre style=\"margin:0\">");
    backtrace(new Path(filename), offset.longValue());
    out.println("</pre></div>");

    dictCache.clear();
    streamCache.clear();
    fileSizeCache.clear();
  }

  private static void backtrace(Path file, long id) throws IOException {
    // recursion based on filename convention
    if (file.getName().startsWith("part-")) {
      file = new Path(file.getParent(),
          "_ramp-" + file.getName().substring(5));
      if (file.getName().charAt(6) == 'm') {
        backtraceMap(file, id, true);
      } else {
        backtraceReduce(file, id);
      }
    } else if (file.getName().startsWith("_ramp-")) {
      file = new Path(file.getParent(),
          "_ramp_map-" + file.getName().substring(6));
      backtraceMap(file, id, false);
    } else {  // reached the original input
      printLine(file, id);
    }
  }

  private static FSDataInputStream getFSDataInputStream(Path file)
      throws IOException {
    FSDataInputStream stream = streamCache.get(file);
    if (stream == null) {
      stream = fs.open(file);
      streamCache.put(file, stream);
    }
    return stream;
  }

  private static long getFileSize(Path file) throws IOException {
    Long size = fileSizeCache.get(file);
    if (size == null) {
      size = fs.getFileStatus(file).getLen();
      fileSizeCache.put(file, size);
    }
    return size;
  }

  private static void backtraceMap(Path file, long id, boolean mapOnly)
      throws IOException {
    // open the file
    FSDataInputStream in = getFSDataInputStream(file);

    // binary search until [start, end) becomes small enough
    // [start, end) defines the "interesting" range
    long start = 0;  // always BLK_SIZE aligned
    long end = getFileSize(file);

    while (end - start > 2 * BLK_SIZE) {
      long middle = ((start + end) / 2) & ~(BLK_SIZE - 1);
      assert(middle >= start + BLK_SIZE);
      in.seek(middle);

      if (WritableUtils.readVLong(in) < id) {
        start = middle;
      } else {
        end = middle;
      }
    }

    // linear search
    in.seek(start);

    for (;;) {
      try {
        long lineId = WritableUtils.readVLong(in);
        int fileId = WritableUtils.readVInt(in);
        long position = WritableUtils.readVLong(in);

        if (lineId == id) {  // found
          backtrace(getInputFileByID(file, fileId), position);

          // only MAP_INTERMEDIATE output can have multiple entries
          // with the same ID.
          if (mapOnly) {
            return;
          }
        } else if (lineId > id) {
          break;
        }
      } catch (EOFException e) {
        break;
      }

      // skip padding at the end of each block
      int unreadBytesInBlock = BLK_SIZE - (int)(in.getPos() & (BLK_SIZE - 1));
      if (unreadBytesInBlock < 23) {
        while (unreadBytesInBlock > 0) {
          unreadBytesInBlock -= in.skipBytes(unreadBytesInBlock);
        }
      }
    }

    if (mapOnly) {
      throw new RuntimeException("Provenance file " + file +
          " does not contain information about position " + id + ".");
    }
  }

  private static Path getInputFileByID(Path file, int fileId)
      throws IOException {
    file = new Path(file.getParent(),
        "_ramp_dict" + file.getName().substring(file.getName().indexOf('-')));

    HashMap<Integer, String> dict = dictCache.get(file);
    if (dict == null) {
      // read the entire dictionary into main memory
      dict = new HashMap<Integer, String>();
      dictCache.put(file, dict);

      FSDataInputStream fileIn = fs.open(file);
      Text line = new Text();
      LineReader in = new LineReader(fileIn);
      while (in.readLine(line) != 0) {
        StringTokenizer itr = new StringTokenizer(line.toString());
        dict.put(Integer.parseInt(itr.nextToken()), itr.nextToken());
      }
      fileIn.close();
    }

    return new Path(dict.get(fileId));
  }

  private static void backtraceReduce(Path file, long position)
      throws IOException {
    // open the file
    FSDataInputStream in = getFSDataInputStream(file);

    // binary search until [start, end) becomes small enough
    // [start, end) defines the "interesting" range
    long start = 0;  // always BLK_SIZE aligned
    long end = getFileSize(file);

    while (end - start > 2 * BLK_SIZE) {
      long middle = ((start + end) / 2) & ~(BLK_SIZE - 1);
      assert(middle >= start + BLK_SIZE);
      in.seek(middle);

      if (WritableUtils.readVLong(in) < position) {
        start = middle;
      } else {
        end = middle;
      }
    }

    // linear search
    in.seek(start);

    for (;;) {
      try {
        long pos = WritableUtils.readVLong(in);
        long provenanceId = WritableUtils.readVLong(in);

        if (pos == position) {  // found
          backtrace(file, provenanceId);
          return;
        } else if (pos > position) {
          break;
        }
      } catch (EOFException e) {
        break;
      }

      // skip padding at the end of each block
      int unreadBytesInBlock = BLK_SIZE - (int)(in.getPos() & (BLK_SIZE - 1));
      if (unreadBytesInBlock < 18) {
        while (unreadBytesInBlock > 0) {
          unreadBytesInBlock -= in.skipBytes(unreadBytesInBlock);
        }
      }
    }

    throw new RuntimeException("Provenance file " + file +
        " does not contain information about position " + position + ".");
  }

  private static void printLine(Path file, long position)
      throws IOException {
    FSDataInputStream fileIn = getFSDataInputStream(file);
    LineReader in = new LineReader(fileIn);
    fileIn.seek(position);
    Text line = new Text();

    if (in.readLine(line) != 0) {
//    jspOut.println(file.getName() + "@" + position +  "\t" + StringEscapeUtils.escapeHtml(line.toString()));
      jspOut.println(StringEscapeUtils.escapeHtml(line.toString()));
    }
  }

}
