/*
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
package utils;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.internal.StaticCredentialsProvider;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.PipelineResult;
import com.google.cloud.dataflow.sdk.io.BigQueryIO;
import com.google.cloud.dataflow.sdk.io.KinesisIO;
import com.google.cloud.dataflow.sdk.io.PubsubIO;
import com.google.cloud.dataflow.sdk.options.DataflowPipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.repackaged.com.google.common.base.Charsets;
import com.google.cloud.dataflow.sdk.runners.DataflowPipelineJob;
import com.google.cloud.dataflow.sdk.runners.DataflowPipelineRunner;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.transforms.windowing.FixedWindows;
import com.google.cloud.dataflow.sdk.transforms.windowing.Window;
import com.google.cloud.dataflow.sdk.values.PCollection;
import org.slf4j.bridge.SLF4JBridgeHandler;

import java.math.BigInteger;
import java.security.SecureRandom;
import java.util.List;

import static com.google.api.client.repackaged.com.google.common.base.Preconditions.checkNotNull;
import static com.google.api.client.util.Lists.newArrayList;
import static java.util.Arrays.asList;
import static org.joda.time.Duration.standardDays;
import static org.joda.time.Duration.standardSeconds;

/***
 *
 */
public class TestUtils {

    static {
        SLF4JBridgeHandler.removeHandlersForRootLogger();

        SLF4JBridgeHandler.install();
    }

    public static final SecureRandom RANDOM = new SecureRandom();

    public static String randomString() {
        return new BigInteger(130, RANDOM).toString(32);
    }

    public static List<String> randomStrings(int howMany) {
        return randomStrings(howMany, 0);
    }

    public static List<String> randomStrings(int howMany, int startingId) {
        List<String> data = newArrayList();
        for (int i = startingId; i < startingId + howMany; ++i) {
            data.add(String.format("%s - %s", i, TestUtils.randomString()));
        }
        return data;
    }

    public static TableSchema getTestTableSchema() {
        return new TableSchema().
                setFields(asList(
                        new TableFieldSchema()
                                .setName("a")
                                .setType("STRING")));
    }

    public static TableReference getTestTableReference() {
        return new TableReference().
                setProjectId(TestConfiguration.get().getTestProject()).
                setDatasetId(TestConfiguration.get().getTestDataset()).
                setTableId(getTestTableId());
    }

    public static String getTestTableId() {
        return randomString();
    }

    public static AWSCredentialsProvider getTestAwsCredentialsProvider() {
        return getStaticCredentialsProvider(
                TestConfiguration.get().getAwsAccessKey(),
                TestConfiguration.get().getAwsSecretKey()
        );
    }

    private static AWSCredentialsProvider getStaticCredentialsProvider(String accessKey,
                                                                       String secretKey) {
        return new StaticCredentialsProvider(new BasicAWSCredentials(
                accessKey, secretKey
        ));
    }

    public static DataflowPipelineOptions getTestPipelineOptions() {
        DataflowPipelineOptions options = PipelineOptionsFactory.as(DataflowPipelineOptions.class);
        options.setProject(TestConfiguration.get().getTestProject());
        options.setStreaming(true);
        options.setJobName(getJobName());
        options.setRunner(DataflowPipelineRunner.class);
        options.setStagingLocation(TestConfiguration.get().getTestStagingLocation());
        options.setTempLocation(TestConfiguration.get().getTestTempLocation());
        return options;
    }

    public static String getJobName() {
        return "e2eKinesisConnectorCorrectness";
    }

    public static DataflowPipelineJob runKinesisToBigQueryJob(TableReference targetTable)
            throws InterruptedException {
        DataflowPipelineOptions options = getTestPipelineOptions();
        Pipeline p = Pipeline.create(options);
        PCollection<String> input = p.
                apply(KinesisIO.Read.
                        from(
                                TestConfiguration.get().getTestKinesisStream(),
                                InitialPositionInStream.LATEST).
                        using(getTestKinesisClientProvider())).
                apply(ParDo.of(new ByteArrayToString()));

        return runBqJob(targetTable, options, p, input);
    }

    public static DataflowPipelineJob runPubSubToBigQueryJob(TableReference targetTable)
            throws InterruptedException {
        DataflowPipelineOptions options = getTestPipelineOptions();
        Pipeline p = Pipeline.create(options);
        PCollection<String> input = p.apply(PubsubIO.Read.topic(TestConfiguration.get()
                .getTestPubSubTopic()));

        return runBqJob(targetTable, options, p, input);
    }

    private static DataflowPipelineJob runBqJob(TableReference targetTable,
                                                DataflowPipelineOptions options, Pipeline p,
                                                PCollection<String> input) throws
            InterruptedException {
        input.apply(Window.<String>into(FixedWindows.of(standardSeconds(10))).
                withAllowedLateness(standardDays(1))).
                apply(ParDo.of(new ToTableRow())).
                apply(BigQueryIO.Write.
                        to(targetTable).
                        withSchema(TestUtils.getTestTableSchema()));
        DataflowPipelineJob job = DataflowPipelineRunner.fromOptions(options).run(p);
        while (job.getState() != PipelineResult.State.RUNNING) {
            Thread.sleep(1000);
        }
        Thread.sleep(1000 * 60 * 5);
        return job;
    }

    public static TestKinesisClientProvider getTestKinesisClientProvider() {
        return new TestKinesisClientProvider();
    }

    /***
     *
     */
    public static class ToTableRow extends DoFn<String, TableRow> {
        @Override
        public void processElement(ProcessContext c) throws Exception {
            checkNotNull(c.element());
            c.output(new TableRow().set("a", c.element()));
        }
    }

    /***
     *
     */
    public static class ByteArrayToString extends DoFn<byte[], String> {
        @Override
        public void processElement(ProcessContext c) throws Exception {
            checkNotNull(c.element());
            c.output(new String(c.element(), Charsets.UTF_8));
        }
    }
}
