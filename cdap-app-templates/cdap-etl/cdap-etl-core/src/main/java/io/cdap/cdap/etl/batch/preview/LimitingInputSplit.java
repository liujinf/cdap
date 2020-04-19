/*
 * Copyright © 2020 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package io.cdap.cdap.etl.batch.preview;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.io.serializer.SerializationFactory;
import org.apache.hadoop.io.serializer.Serializer;
import org.apache.hadoop.mapreduce.InputSplit;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * An {@link InputSplit} that delegates to another {@link InputSplit} and also carries record limit information.
 */
public class LimitingInputSplit extends InputSplit implements Writable, Configurable {

  private InputSplit delegate;
  private int recordLimit;
  private Configuration conf;

  public LimitingInputSplit() {
    // no-op, for deserialization
  }

  LimitingInputSplit(Configuration conf, InputSplit delegate, int recordLimit) {
    this.conf = conf;
    this.delegate = delegate;
    this.recordLimit = recordLimit;
  }

  InputSplit getDelegate() {
    return delegate;
  }

  int getRecordLimit() {
    return recordLimit;
  }

  @Override
  public long getLength() throws IOException, InterruptedException {
    return delegate.getLength();
  }

  @Override
  public String[] getLocations() throws IOException, InterruptedException {
    return delegate.getLocations();
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(recordLimit);
    Text.writeString(out, delegate.getClass().getName());

    Serializer serializer = new SerializationFactory(getConf()).getSerializer(delegate.getClass());
    serializer.open((DataOutputStream) out);
    //noinspection unchecked
    serializer.serialize(delegate);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    recordLimit = in.readInt();
    String className = Text.readString(in);
    try {
      Class<? extends InputSplit> cls = getConf().getClassLoader().loadClass(className).asSubclass(InputSplit.class);
      Deserializer deserializer = new SerializationFactory(getConf()).getDeserializer(cls);
      deserializer.open((DataInputStream) in);
      //noinspection unchecked
      delegate = (InputSplit) deserializer.deserialize(cls);
    } catch (ClassNotFoundException e) {
      throw new IOException("Failed to load InputSplit class " + className, e);
    }
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public Configuration getConf() {
    return conf;
  }
}
