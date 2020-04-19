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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

/**
 *
 */
@RunWith(Parameterized.class)
public class LimitingInputFormatTest {

  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

  @Parameterized.Parameters(name = "{index} : max split = {0}")
  public static Collection<Object[]> data() {
    return Arrays.asList(
      new Object[] { 5 },
      new Object[] { 25 },
      new Object[] { 100 },
      new Object[] { 2000 }
    );
  }

  private final int maxRecords;
  private File inputDir;

  public LimitingInputFormatTest(int maxRecords) {
    this.maxRecords = maxRecords;
  }

  @Before
  public void generateInput() throws IOException {
    File inputDir = TEMP_FOLDER.newFolder();

    // Generate 10 files, each with 100 lines
    for (int i = 0; i < 10; i++) {
      File file = new File(inputDir, "file" + i + ".txt");
      try (BufferedWriter writer = Files.newBufferedWriter(file.toPath())) {
        for (int j = 0; j < 100; j++) {
          writer.write("Testing " + i + "," + j);
          writer.newLine();
        }
      }
    }
    this.inputDir = inputDir;
  }

  @Test
  public void testSplits() throws IOException, InterruptedException {
    Configuration hConf = new Configuration();
    hConf.set(LimitingInputFormat.DELEGATE_CLASS_NAME, TextInputFormat.class.getName());
    hConf.setInt(LimitingInputFormat.MAX_RECORDS, maxRecords);

    // Create splits from the given directory
    Job job = Job.getInstance(hConf);
    job.setJobID(new JobID("test", 0));
    FileInputFormat.addInputPath(job, new Path(inputDir.toURI()));

    LimitingInputFormat<String, String> inputFormat = new LimitingInputFormat<>();
    inputFormat.setConf(hConf);

    List<InputSplit> splits = inputFormat.getSplits(job);
    // We have 10 files as input. If maxRecords < 10, then there should be maxRecords splits.
    int expectedSplitSize = Math.min(maxRecords, 10);
    Assert.assertEquals(expectedSplitSize, splits.size());

    int totalRecords = splits.stream()
      .filter(LimitingInputSplit.class::isInstance)
      .map(LimitingInputSplit.class::cast)
      .map(LimitingInputSplit::getRecordLimit)
      .reduce(Integer::sum)
      .orElse(-1);
    // Maximum number of records is 1000.
    Assert.assertEquals(Math.min(1000, maxRecords), totalRecords);
  }

  @Test
  public void testSingleSplit() throws IOException, InterruptedException {
    Configuration hConf = new Configuration();
    hConf.set(LimitingInputFormat.DELEGATE_CLASS_NAME, TextInputFormat.class.getName());
    hConf.setInt(LimitingInputFormat.MAX_RECORDS, maxRecords);

    // Use one file to create split.
    Job job = Job.getInstance(hConf);
    job.setJobID(new JobID("test", 0));
    File[] files = inputDir.listFiles();
    Assert.assertNotNull(files);
    Assert.assertTrue(files.length > 0);
    FileInputFormat.addInputPath(job, new Path(files[0].toURI()));

    LimitingInputFormat<String, String> inputFormat = new LimitingInputFormat<>();
    inputFormat.setConf(hConf);

    List<InputSplit> splits = inputFormat.getSplits(job);
    Assert.assertEquals(1, splits.size());

    InputSplit inputSplit = splits.get(0);
    Assert.assertTrue(inputSplit instanceof LimitingInputSplit);

    // For single split case, we don't read the data, hence the limit would be the same as max records.
    Assert.assertEquals(maxRecords, ((LimitingInputSplit) inputSplit).getRecordLimit());
  }
}
