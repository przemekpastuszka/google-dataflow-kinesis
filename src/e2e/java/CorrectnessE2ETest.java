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

import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.compute.model.Instance;
import com.google.cloud.dataflow.sdk.PipelineResult;
import com.google.cloud.dataflow.sdk.repackaged.com.google.common.collect.Lists;
import com.google.cloud.dataflow.sdk.repackaged.com.google.common.collect.Sets;
import com.google.cloud.dataflow.sdk.runners.DataflowPipelineJob;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import utils.BQ;
import utils.GCE;
import utils.TestConfiguration;
import utils.TestUtils;
import utils.kinesis.KinesisUploader;
import utils.kinesis.KinesisUploaderProvider;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.google.api.client.repackaged.com.google.common.base.Strings.commonPrefix;
import static com.google.cloud.dataflow.sdk.repackaged.com.google.common.collect.Sets.newHashSet;
import static java.lang.System.currentTimeMillis;
import static org.fest.assertions.Assertions.assertThat;

/**
 * Created by ppastuszka on 12.12.15.
 */
public class CorrectnessE2ETest {
    private static final Logger LOG = LoggerFactory.getLogger(CorrectnessE2ETest.class);

    private TableReference testTable;
    private DataflowPipelineJob job;

    @BeforeMethod
    public void setUp() throws IOException {
        job = null;
        testTable = TestUtils.getTestTableReference();
        BQ.get().deleteTableIfExists(testTable);
        LOG.info("Creating table" + testTable);
        BQ.get().createTable(testTable, TestUtils.getTestTableSchema());
    }

    @AfterMethod
    public void tearDown() throws IOException, InterruptedException {
        LOG.info("Deleting table" + testTable);
        BQ.get().deleteTableIfExists(testTable);
        if (job != null) {
            job.cancel();
            while (job.getState() != PipelineResult.State.CANCELLED) {
                LOG.info("Waiting for job to finish. Current state is {}", job.getState());
                Thread.sleep(5000);
            }
        }
    }


    @Test(dataProviderClass = KinesisUploaderProvider.class, dataProvider = "provide", enabled =
            false)
    public void testSimpleCorrectnessOnDataflowService(KinesisUploader client) throws
            InterruptedException,
            IOException, TimeoutException {
        job = TestUtils.runKinesisToBigQueryJob(testTable);
        LOG.info("Sending events to kinesis");

        List<String> testData = TestUtils.randomStrings(20000);
        client.startUploadingRecords(testData).waitForFinish(Long.MAX_VALUE);

        verifyDataPresentInBigQuery(testData, TimeUnit.MINUTES.toMillis(2));
    }

    @Test(dataProviderClass = KinesisUploaderProvider.class, dataProvider = "provide",
            invocationCount = 10)
    public void dealsWithInstanceBeingRestarted(KinesisUploader client) throws
            InterruptedException, IOException,
            TimeoutException {
        job = TestUtils.runKinesisToBigQueryJob(testTable);
        LOG.info("Sending events to kinesis");

        List<String> testData = TestUtils.randomStrings(40000);
        KinesisUploader.RecordUploadFuture future = client.startUploadingRecords(testData);
        Instance randomInstance = chooseRandomInstance();
        GCE.get().stopInstance(randomInstance);
        future.waitForFinish(Long.MAX_VALUE);

        List<String> newTestData = TestUtils.randomStrings(40000, 40000);
        future = client.startUploadingRecords(newTestData);
        testData.addAll(newTestData);
        GCE.get().startInstance(randomInstance);
        future.waitForFinish(Long.MAX_VALUE);

        verifyDataPresentInBigQuery(testData, TimeUnit.MINUTES.toMillis(6));
    }

    private Instance chooseRandomInstance() throws IOException {
        List<Instance> currentDataflowInstances = getCurrentDataflowInstances();
        int randomIndex = TestUtils.RANDOM.nextInt(currentDataflowInstances.size());
        return currentDataflowInstances.get(randomIndex);
    }

    private List<Instance> getCurrentDataflowInstances() throws IOException {
        List<Instance> allInstances = GCE.get()
                .listInstances(TestConfiguration.get().getTestProject());

        List<Instance> currentDataflowInstances = Lists.newArrayList();
        for (Instance instance : allInstances) {
            String prefix = commonPrefix(TestUtils.getJobName().toLowerCase(), instance.getName()
                    .toLowerCase());
            if (prefix.length() >= 20) {
                currentDataflowInstances.add(instance);
            }
        }
        return currentDataflowInstances;
    }


    private void verifyDataPresentInBigQuery(List<String> testData, long timeout) throws
            IOException,
            InterruptedException {
        LOG.info("Waiting for pipeline to process all sent data");

        long sleepPeriod = TimeUnit.SECONDS.toMillis(30);
        long startTime = currentTimeMillis();
        AssertionError lastException = null;
        while (currentTimeMillis() - startTime <= timeout) {
            try {
                verifySingleDataInBigQuery(testData);
                return;
            } catch (AssertionError e) {
                lastException = e;
                LOG.warn("Data in BigQuery not yet ready", e);
                Thread.sleep(sleepPeriod);
            }
        }
        throw lastException;
    }

    private void verifySingleDataInBigQuery(List<String> testData) throws IOException {
        LOG.info("Veryfing result in BigQuery");
        List<String> dataFromBQ = BQ.get().readAllFrom(testTable);
        HashSet<String> setOfExpectedData = newHashSet(testData);
        HashSet<String> setOfDataInBQ = newHashSet(dataFromBQ);

        Set<String> dataNotInBQ = Sets.difference(setOfExpectedData, setOfDataInBQ);
        Set<String> redundantDataInBQ = Sets.difference(setOfDataInBQ, setOfExpectedData);

        assertThat(dataNotInBQ).isEmpty();
        assertThat(redundantDataInBQ).isEmpty();
    }
}
