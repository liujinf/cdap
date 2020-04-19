/*
 * Copyright © 2019 Cask Data, Inc.
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
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * A wrapper around another input format, that limits the amount of data read.
 *
 * @param <K> type of key to read
 * @param <V> type of value to read
 */
public class LimitingInputFormat<K, V> extends InputFormat<K, V> implements Configurable {

  static final String DELEGATE_CLASS_NAME = "io.cdap.pipeline.preview.input.classname";
  static final String MAX_RECORDS = "io.cdap.pipeline.preview.max.records";

  private InputFormat<K, V> delegateFormat;
  private Configuration conf;

  @Override
  public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException {
    Configuration conf = context.getConfiguration();
    int maxRecords = conf.getInt(MAX_RECORDS, 100);

    List<InputSplit> splits = createDelegate(conf).getSplits(context);
    if (splits.size() <= 1) {
      int limit = maxRecords;
      return splits.stream().map(s -> new LimitingInputSplit(conf, s, limit)).collect(Collectors.toList());
    }

    // If there are more than one splits, try to read records from each split to determine what's the actual split
    // limit per split.
    Map<InputSplit, Integer> recordLimits = new IdentityHashMap<>();
    TaskID taskId = new TaskID(context.getJobID(), TaskType.JOB_SETUP, 0);
    TaskAttemptContext taskContext = new TaskAttemptContextImpl(conf, new TaskAttemptID(taskId, 0));
    List<InputSplit> activeSplits = new LinkedList<>(splits);
    while (maxRecords > 0 && !activeSplits.isEmpty()) {
      int recordPerSplit = Math.max(1, maxRecords / activeSplits.size());

      Iterator<InputSplit> iterator = activeSplits.iterator();
      while (iterator.hasNext()) {
        InputSplit split = iterator.next();

        // We close the record reader in each iteration to avoid keeping all of them open to preserve memory
        // in case there are a lot of partitions.
        try (RecordReader<K, V> reader = createRecordReader(split, taskContext)) {
          reader.initialize(split, taskContext);
          int consumed = recordLimits.getOrDefault(split, 0);
          int skipped = skipRecords(reader, consumed + recordPerSplit);

          // If we skipped more than what's being consumed in last iteration, we need to include more records
          // from this input.
          if (skipped > consumed) {
            maxRecords -= (skipped - consumed);
            recordLimits.put(split, skipped);
          }

          if (!reader.nextKeyValue()) {
            iterator.remove();
          }
        }
        if (maxRecords <= 0) {
          break;
        }
      }
    }

    // Loop the map with the original input split order to construct the final input splits
    List<InputSplit> resultSplits = new ArrayList<>();
    for (InputSplit split : splits) {
      Integer recordLimit = recordLimits.get(split);
      if (recordLimit == null || recordLimit <= 0) {
        continue;
      }
      resultSplits.add(new LimitingInputSplit(conf, split, recordLimit));
    }
    return resultSplits;
  }

  @Override
  public RecordReader<K, V> createRecordReader(InputSplit split,
                                               TaskAttemptContext context) throws IOException, InterruptedException {
    Configuration conf = context.getConfiguration();
    int limit = conf.getInt(MAX_RECORDS, 100);
    InputSplit readerSplit = split;
    if (split instanceof LimitingInputSplit) {
      limit = ((LimitingInputSplit) split).getRecordLimit();
      readerSplit = ((LimitingInputSplit) split).getDelegate();
    }
    RecordReader<K, V> delegate = delegateFormat.createRecordReader(readerSplit, context);
    return new LimitingRecordReader<>(delegate, limit);
  }

  /**
   * Skips the given number of records from the given {@link RecordReader}.
   *
   * @param reader the {@link RecordReader} to read from
   * @param skip number of records to skip
   * @return the number of records being skipped
   */
  private int skipRecords(RecordReader<K, V> reader, int skip) throws IOException, InterruptedException {
    int count = 0;
    while (count != skip && reader.nextKeyValue()) {
      count++;
    }
    return count;
  }

  private InputFormat<K, V> createDelegate(Configuration conf) throws IOException {
    String delegateClassName = conf.get(DELEGATE_CLASS_NAME);
    try {
      //noinspection unchecked
      return (InputFormat<K, V>) conf.getClassLoader().loadClass(delegateClassName).newInstance();
    } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
      throw new IOException("Unable to instantiate delegate input format " + delegateClassName, e);
    }
  }

  @Override
  public void setConf(Configuration conf) {
    try {
      delegateFormat = createDelegate(conf);
      if (delegateFormat instanceof Configurable) {
        ((Configurable) delegateFormat).setConf(conf);
      }
      this.conf = conf;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Configuration getConf() {
    return conf;
  }
}
