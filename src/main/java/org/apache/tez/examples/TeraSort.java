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

package org.apache.tez.examples;

import java.io.IOException;

import java.net.URI;
import org.apache.hadoop.examples.terasort.TeraInputFormat;
import org.apache.hadoop.examples.terasort.TeraOutputFormat;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.task.JobContextImpl;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.tez.common.TezRuntimeFrameworkConfigs;
import org.apache.tez.mapreduce.lib.MRReaderMapReduce;
import org.apache.tez.mapreduce.partition.MRPartitioner;
import org.apache.tez.runtime.library.api.KeyValueReader;
import org.apache.tez.runtime.library.common.comparator.TezTextComparator;
import org.apache.tez.runtime.library.common.serializer.TezTextSerialization;
import org.apache.tez.runtime.library.input.OrderedGroupedInputLegacy;
import org.apache.tez.runtime.library.input.OrderedGroupedKVInput;
import org.apache.tez.runtime.library.output.OrderedPartitionedKVOutput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.ToolRunner;
import org.apache.tez.client.TezClient;
import org.apache.tez.dag.api.DAG;
import org.apache.tez.dag.api.DataSinkDescriptor;
import org.apache.tez.dag.api.DataSourceDescriptor;
import org.apache.tez.dag.api.Edge;
import org.apache.tez.dag.api.ProcessorDescriptor;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.Vertex;
import org.apache.tez.examples.WordCount.TokenProcessor;
import org.apache.tez.mapreduce.input.MRInput;
import org.apache.tez.mapreduce.output.MROutput;
import org.apache.tez.mapreduce.processor.SimpleMRProcessor;
import org.apache.tez.runtime.api.ProcessorContext;
import org.apache.tez.runtime.library.api.KeyValueWriter;
import org.apache.tez.runtime.library.api.KeyValuesReader;
import org.apache.tez.runtime.library.conf.OrderedPartitionedKVEdgeConfig;

import com.google.common.base.Preconditions;

/**
 * Simple example that extends the WordCount example to show a chain of processing.
 * The example extends WordCount by sorting the words by their count.
 */
public class TeraSort extends TezExampleBase {
  public static final String PARTITION_FILENAME = "_partition.lst";
  
  private static String INPUT = "Input";
  private static String OUTPUT = "Output";
  private static String TERASORT_MAPPER = "TeraSortMapper";
  private static String TERASORT_REDUCER = "TeraSortReducer";
  private static final Logger LOG = LoggerFactory.getLogger(TeraSort.class);

  /*
   * SumProcessor similar to WordCount except that it writes the count as key and the 
   * word as value. This is because we can and ordered partitioned key value edge to group the 
   * words with the same count (as key) and order the counts.
   */
  public static class TeraSortMapperProcessor extends SimpleMRProcessor {
    public TeraSortMapperProcessor(ProcessorContext context) {
      super(context);
    }

    @Override
    public void run() throws Exception {
      Preconditions.checkArgument(getInputs().size() == 1);
      Preconditions.checkArgument(getOutputs().size() == 1);
      // the recommended approach is to cast the reader/writer to a specific type instead
      // of casting the input/output. This allows the actual input/output type to be replaced
      // without affecting the semantic guarantees of the data type that are represented by
      // the reader and writer.
      // The inputs/outputs are referenced via the names assigned in the DAG.
      KeyValueWriter kvWriter = ((OrderedPartitionedKVOutput) getOutputs().get(TERASORT_REDUCER)).getWriter();
      KeyValueReader kvReader = (MRReaderMapReduce) getInputs().get(INPUT).getReader();
      while (kvReader.next()) {
        kvWriter.write(kvReader.getCurrentKey(), kvReader.getCurrentValue());
      }
    }
  }
  
  /**
   * No-op sorter processor. It does not need to apply any logic since the ordered partitioned edge 
   * ensures that we get the data sorted and grouped by the the sum key.
   */
  public static class TeraSortReducerProcessor extends SimpleMRProcessor {

    public TeraSortReducerProcessor(ProcessorContext context) {
      super(context);
    }

    @Override
    public void run() throws Exception {
      Preconditions.checkArgument(getInputs().size() == 1);
      Preconditions.checkArgument(getOutputs().size() == 1);

      KeyValueWriter kvWriter = (KeyValueWriter) getOutputs().get(OUTPUT).getWriter();
      KeyValuesReader kvReader = ((OrderedGroupedKVInput) getInputs().get(TERASORT_MAPPER)).getReader();
      while (kvReader.next()) {
        Text k = (Text)kvReader.getCurrentKey();
        Iterable<Text> values = (Iterable) kvReader.getCurrentValues().iterator();
        for (Text v : values) {
          kvWriter.write(k, v);
        }
      }
      // deriving from SimpleMRProcessor takes care of committing the output
    }
  }
  
  public static DAG createDAG(TezConfiguration tezConf, String inputPath, String outputPath,
      int numPartitions, boolean disableSplitGrouping, String dagName) throws IOException {


    long start = System.currentTimeMillis();
    Path partitionFile = new Path(outputPath, PARTITION_FILENAME);
    Job job = Job.getInstance(tezConf);
    TeraInputFormat.setInputPaths(job, new Path(inputPath));
    FileOutputFormat.setOutputPath(job, new Path(outputPath));
    URI partitionUri;
    try {
      partitionUri = new URI(partitionFile.toString() + "#" + PARTITION_FILENAME);
      TeraInputFormat.writePartitionFile(job, partitionFile);
    } catch (Throwable e) {
      LOG.error(e.getMessage());
      throw new IOException(e);
    }
    job.addCacheFile(partitionUri);
    job.setPartitionerClass(TotalOrderPartitioner.class);
    long end = System.currentTimeMillis();
    System.out.println("Spent " + (end - start) + "ms computing partitions.");

    tezConf.setInt(TezRuntimeFrameworkConfigs.TEZ_RUNTIME_NUM_EXPECTED_PARTITIONS, numPartitions);

    DataSourceDescriptor dataSource = MRInput.createConfigBuilder(new Configuration(tezConf),
        TeraInputFormat.class, inputPath).groupSplits(!disableSplitGrouping).build();

    DataSinkDescriptor dataSink = MROutput.createConfigBuilder(new Configuration(tezConf), TeraOutputFormat.class,
        outputPath).build();

    Vertex terasortMapVertex = Vertex.create(TERASORT_MAPPER, ProcessorDescriptor.create(
        TeraSortMapperProcessor.class.getName()));
    terasortMapVertex.addDataSource(INPUT, dataSource);

    // Use Text key and IntWritable value to bring counts for each word in the same partition
    // The setFromConfiguration call is optional and allows overriding the config options with
    // command line parameters.
    OrderedPartitionedKVEdgeConfig TeraSortEdgeConf = OrderedPartitionedKVEdgeConfig
        .newBuilder(Text.class.getName(), Text.class.getName(), MRPartitioner.class.getName())
        .setFromConfiguration(tezConf)
        .setKeySerializationClass(TezTextSerialization.class.getName(), TezTextComparator.class.getName(), null)
        .build();

    // This vertex will be reading intermediate data via an input edge and writing intermediate data
    // via an output edge.
    Vertex terasortReducerVertex = Vertex.create(TERASORT_REDUCER, ProcessorDescriptor.create(
        TeraSortReducerProcessor.class.getName()), numPartitions);
    terasortReducerVertex.addDataSink(OUTPUT, dataSink);

    // No need to add jar containing this class as assumed to be part of the tez jars.
    DAG dag = DAG.create(dagName);
    dag.addVertex(terasortMapVertex)
        .addVertex(terasortReducerVertex)
        .addEdge(Edge.create(terasortMapVertex, terasortReducerVertex, TeraSortEdgeConf.createDefaultEdgeProperty()));



    //job.setPartitionerClass(TotalOrderPartitioner.class);

    return dag;
  }

  @Override
  protected void printUsage() {
    System.err.println("Usage: " + " orderedwordcount in out [numPartitions]");
  }

  @Override
  protected int validateArgs(String[] otherArgs) {
    if (otherArgs.length < 2 || otherArgs.length > 3) {
      return 2;
    }
    return 0;
  }

  @Override
  protected int runJob(String[] args, TezConfiguration tezConf,
      TezClient tezClient) throws Exception {
    DAG dag = createDAG(tezConf, args[0], args[1],
        args.length == 3 ? Integer.parseInt(args[2]) : 1, isDisableSplitGrouping(),
        "TezTeraSort");
    LOG.info("Running TezTeraSort");

    return runDag(dag, true, LOG);
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new TeraSort(), args);
    System.exit(res);
  }
}
