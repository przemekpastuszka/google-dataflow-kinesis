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

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.PipelineResult;
import com.google.cloud.dataflow.sdk.io.KinesisIO;
import com.google.cloud.dataflow.sdk.runners.DirectPipelineRunner;
import com.google.cloud.dataflow.sdk.testing.DataflowAssert;
import com.google.cloud.dataflow.sdk.testing.TestPipeline;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.values.PCollection;

import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import static org.assertj.core.api.Assertions.assertThat;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import static utils.TestUtils.getTestKinesisClientProvider;
import utils.TestConfiguration;
import utils.TestUtils;
import utils.kinesis.KinesisUploaderProvider;
import utils.kinesis.RecordsUploader;

/***
 *
 */
public class CorrectnessIntegrationTest {
    private static final Logger LOG = LoggerFactory.getLogger(CorrectnessIntegrationTest.class);
    private static final long PIPELINE_STARTUP_TIME = TimeUnit.SECONDS.toMillis(10);
    private static final long ADDITIONAL_PROCESSING_TIME = TimeUnit.SECONDS.toMillis(60);
    private static final long RECORD_GENERATION_TIMEOUT = TimeUnit.SECONDS.toMillis(35);
    private static final long TOTAL_PROCESSING_TIME = PIPELINE_STARTUP_TIME +
            RECORD_GENERATION_TIMEOUT +
            ADDITIONAL_PROCESSING_TIME;
    private ExecutorService singleThreadExecutor = Executors.newSingleThreadExecutor();
    private List<String> testData;

    @BeforeTest
    public void setUp() {
        testData = TestUtils.randomStrings(50000);
    }

    @Test(dataProviderClass = KinesisUploaderProvider.class, dataProvider = "provide")
    public void readerTestWithKinesisProducer(RecordsUploader client) throws Exception {
        Future<?> future = startTestPipeline();
        client.startUploadingRecords(testData).waitForFinish(RECORD_GENERATION_TIMEOUT);
        LOG.info("All data sent to kinesis");
        future.get();
    }

    private Future<?> startTestPipeline() throws InterruptedException {
        final Pipeline p = TestPipeline.create();
        ((DirectPipelineRunner) p.getRunner()).getPipelineOptions().setStreaming(true);
        PCollection<String> result = p.
                apply(KinesisIO.Read.
                        from(
                                TestConfiguration.get().getTestKinesisStream(),
                                InitialPositionInStream.LATEST).
                        using(getTestKinesisClientProvider()).
                        withMaxReadTime(Duration.millis(
                                TOTAL_PROCESSING_TIME)
                        )
                ).
                apply(ParDo.of(new TestUtils.RecordDataToString()));
        DataflowAssert.that(result).containsInAnyOrder(testData);

        Future<?> future = singleThreadExecutor.submit(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                PipelineResult result = p.run();
                PipelineResult.State state = result.getState();
                while (state != PipelineResult.State.DONE && state != PipelineResult.State.FAILED) {
                    Thread.sleep(1000);
                    state = result.getState();
                }
                assertThat(state).isEqualTo(PipelineResult.State.DONE);
                return null;
            }
        });
        Thread.sleep(PIPELINE_STARTUP_TIME);
        return future;
    }
}
