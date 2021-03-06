package org.apache.tez.runtime.library.common.serializer;

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

import java.io.DataInputStream;
import java.io.DataOutputStream;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.io.serializer.Serialization;
import org.apache.hadoop.io.serializer.Serializer;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * <pre>
 * When using BytesWritable, data is serialized in memory (4 bytes per key and 4 bytes per value)
 * and written to IFile where it gets serialized again (4 bytes per key and 4 bytes per value).
 * This adds an overhead of 8 bytes per key value pair written. This class reduces this overhead
 * by providing a fast serializer/deserializer to speed up inner loop of sort,
 * spill, merge.
 *
 * Usage e.g:
 *  OrderedPartitionedKVEdgeConfig edgeConf = OrderedPartitionedKVEdgeConfig
 *         .newBuilder(keyClass, valClass, MRPartitioner.class.getName(), partitionerConf)
 *         .setFromConfiguration(conf)
 *         .setKeySerializationClass(TezBytesWritableSerialization.class.getName(),
 *            TezBytesComparator.class.getName()).build())
 * </pre>
 */
@Public
@Unstable
public class TezTextSerialization extends Configured implements Serialization<Writable> {

  private static final Logger LOG = LoggerFactory.getLogger(TezTextSerialization.class.getName());

  public boolean accept(Class<?> c) {
    return (Text.class.isAssignableFrom(c));
  }

  public Serializer<Writable> getSerializer(Class<Writable> c) {
    return new TezTextSerializer();
  }

  public Deserializer<Writable> getDeserializer(Class<Writable> c) {
    return new TezTextDeserializer(getConf(), c);
  }

  public static class TezTextDeserializer extends Configured
      implements Deserializer<Writable> {
    private Class<?> writableClass;
    private DataInputStream dataInputStream;

    public TezTextDeserializer(Configuration conf, Class<?> c) {
      setConf(conf);
      this.writableClass = c;
    }

    public void open(InputStream in) {
      dataInputStream = new DataInputStream(in);
    }

    public Writable deserialize(Writable w) throws IOException {
      Text writable = (Text) w;
      if (w == null) {
        writable = (Text) ReflectionUtils.newInstance(writableClass, getConf());
      }

      writable.readFields(dataInputStream);
      return writable;
    }

    public void close() throws IOException {
      //dataIn.close();
      dataInputStream.close();
    }

  }

  public static class TezTextSerializer extends Configured implements
      Serializer<Writable> {

    private DataOutputStream dataOutputStream;

    public void open(OutputStream out) {
      dataOutputStream = new DataOutputStream(out);
    }

    public void serialize(Writable w) throws IOException {
      Text writable = (Text) w;
      writable.write(dataOutputStream);
    }

    public void close() throws IOException {
      //dataOut.close();
      dataOutputStream.close();
    }
  }
}

