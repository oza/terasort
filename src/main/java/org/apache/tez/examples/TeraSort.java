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

import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.security.TokenCache;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.util.ClassUtil;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.tez.client.TezClientUtils;
import org.apache.tez.common.TezRuntimeFrameworkConfigs;
import org.apache.tez.dag.api.TezUncheckedException;
import org.apache.tez.mapreduce.lib.MRReaderMapReduce;
import org.apache.tez.mapreduce.partition.MRPartitioner;
import org.apache.tez.runtime.library.api.KeyValueReader;
import org.apache.tez.runtime.library.common.comparator.TezTextComparator;
import org.apache.tez.runtime.library.common.serializer.TezTextSerialization;
import org.apache.tez.runtime.library.input.OrderedGroupedKVInput;
import org.apache.tez.runtime.library.output.OrderedPartitionedKVOutput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
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
        Iterable<Text> values = (Iterable) kvReader.getCurrentValues();
        for (Text v : values) {
          kvWriter.write(k, v);
        }
      }
      // deriving from SimpleMRProcessor takes care of committing the output
    }
  }

  public static void createPartitionFile(Configuration conf, Path inputPath,
      Path partitionFile, int numPartitions) throws IOException {
    // Dirty hack for reusing TeraInputFormat.writePartitionFile
    conf.setInt(TeraInputFormat.NUM_PARTITIONS, numPartitions);
    //conf.setClass("mapreduce.job.partitioner.class", TotalOrderPartitioner.class,
    //    Partitioner.class);
    Job job = Job.getInstance(conf);
    TeraInputFormat.setInputPaths(job, inputPath);
    try {
      TeraInputFormat.writePartitionFile(job, partitionFile, numPartitions);
    } catch (Throwable e) {
      LOG.error(e.getMessage());
      throw new IOException(e);
    }
  }
  
  public static DAG createDAG(TezConfiguration tezConf, String inputPath, String outputPath,
      int numPartitions, boolean disableSplitGrouping, String dagName) throws IOException {

    FileSystem fs = FileSystem.get(tezConf);
    if (fs.exists(new Path(outputPath))) {
      throw new IOException("Output directory : " + outputPath + " already exists");
    }

    Path stagingDir = new Path(fs.getWorkingDirectory(), UUID.randomUUID().toString());
    tezConf.set(TezConfiguration.TEZ_AM_STAGING_DIR, stagingDir.toString());
    TezClientUtils.ensureStagingDirExists(tezConf, stagingDir);

    String jarPath = ClassUtil.findContainingJar(TeraSort.class);
    if (jarPath == null) {
      throw new TezUncheckedException("Could not find any jar containing"
          + TeraSort.class.getName() + " in the classpath");
    }

    Credentials credentials = new Credentials();
    Path remoteJarPath = fs.makeQualified(new Path(stagingDir, "dag_job.jar"));
    fs.copyFromLocalFile(new Path(jarPath), remoteJarPath);
    FileStatus remoteJarStatus = fs.getFileStatus(remoteJarPath);
    TokenCache.obtainTokensForNamenodes(credentials, new Path[]{remoteJarPath}, tezConf);

    Map<String, LocalResource> commonLocalResources = new TreeMap<String, LocalResource>();
    LocalResource dagJarLocalRsrc = LocalResource
        .newInstance(ConverterUtils.getYarnUrlFromPath(remoteJarPath), LocalResourceType.FILE,
            LocalResourceVisibility.APPLICATION, remoteJarStatus.getLen(),
        remoteJarStatus.getModificationTime());
    commonLocalResources.put("dag_job.jar", dagJarLocalRsrc);

    long start = System.currentTimeMillis();

    Path partitionFile = new Path(outputPath, PARTITION_FILENAME);
    createPartitionFile(tezConf, new Path(inputPath), partitionFile, numPartitions);
    // Registering partitionFile as LocalResources
    Path remotePartitionFilePath = fs.makeQualified(partitionFile);
    FileStatus remotePartitionFileStatus = fs.getFileStatus(remotePartitionFilePath);
    Map<String, LocalResource> terasortLocalResources = new TreeMap<String, LocalResource>();
    LocalResource partitionFileLocalRsrc = LocalResource.newInstance(
        ConverterUtils.getYarnUrlFromPath(remotePartitionFilePath), LocalResourceType.FILE,
        LocalResourceVisibility.APPLICATION, remotePartitionFileStatus.getLen(),
        remotePartitionFileStatus.getModificationTime());
    terasortLocalResources.put(PARTITION_FILENAME, partitionFileLocalRsrc);
    long end = System.currentTimeMillis();
    System.out.println("Spent " + (end - start) + "ms computing partitions.");

    tezConf.setInt(TezRuntimeFrameworkConfigs.TEZ_RUNTIME_NUM_EXPECTED_PARTITIONS, numPartitions);

    DataSourceDescriptor dataSource = MRInput.createConfigBuilder(new Configuration(tezConf),
        TeraInputFormat.class, inputPath).groupSplits(!disableSplitGrouping).build();

    DataSinkDescriptor dataSink = MROutput.createConfigBuilder(new Configuration(tezConf),
        TeraOutputFormat.class, outputPath).build();

    Vertex terasortMapVertex = Vertex.create(TERASORT_MAPPER, ProcessorDescriptor.create(
        TeraSortMapperProcessor.class.getName()))
        .addTaskLocalFiles(commonLocalResources)
        .addTaskLocalFiles(terasortLocalResources);
    terasortMapVertex.addDataSource(INPUT, dataSource);

    // Use Text key and IntWritable value to bring counts for each word in the same partition
    // The setFromConfiguration call is optional and allows overriding the config options with
    // command line parameters.
    OrderedPartitionedKVEdgeConfig teraSortEdgeConf = OrderedPartitionedKVEdgeConfig
        .newBuilder(Text.class.getName(), Text.class.getName(), TotalOrderPartitioner.class.getName())
        .setFromConfiguration(tezConf)
        .setKeySerializationClass(TezTextSerialization.class.getName(), TezTextComparator.class.getName(), null)
        .setValueSerializationClass(TezTextSerialization.class.getName(), null)
        .build();

    // This vertex will be reading intermediate data via an input edge and writing intermediate data
    // via an output edge.
    Vertex terasortReducerVertex = Vertex.create(TERASORT_REDUCER, ProcessorDescriptor.create(
        TeraSortReducerProcessor.class.getName()), numPartitions)
        .addTaskLocalFiles(commonLocalResources)
        .addTaskLocalFiles(terasortLocalResources);
    terasortReducerVertex.addDataSink(OUTPUT, dataSink);

    // No need to add jar containing this class as assumed to be part of the tez jars.
    DAG dag = DAG.create(dagName);
    dag.addVertex(terasortMapVertex)
        .addVertex(terasortReducerVertex)
        .addEdge(
            Edge.create(terasortMapVertex, terasortReducerVertex, teraSortEdgeConf.createDefaultEdgeProperty()));

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
