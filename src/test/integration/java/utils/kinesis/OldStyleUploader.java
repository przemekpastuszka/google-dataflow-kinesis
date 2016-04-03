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
package utils.kinesis;

import static com.google.api.client.util.Lists.newArrayList;
import com.google.cloud.dataflow.sdk.repackaged.com.google.common.base.Charsets;
import com.google.cloud.dataflow.sdk.repackaged.com.google.common.collect.Lists;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.model.PutRecordsRequest;
import com.amazonaws.services.kinesis.model.PutRecordsRequestEntry;
import com.amazonaws.services.kinesis.model.PutRecordsResult;
import com.amazonaws.services.kinesis.model.PutRecordsResultEntry;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import utils.TestConfiguration;
import utils.TestUtils;

/**
 *
 */
public class OldStyleUploader implements KinesisUploader {
    private final ExecutorService executor = Executors.newSingleThreadExecutor();

    @Override
    public RecordUploadFuture startUploadingRecords(final List<String> data) {
        return new RecordUploaderFuture(executor.submit(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                uploadAll(data);
                return null;
            }
        }));
    }

    @Override
    public String toString() {
        return "Old-style puts uploader";
    }

    private void uploadAll(List<String> data) {
        List<List<String>> partitions = Lists.partition(data, 499);

        AmazonKinesisClient client = new AmazonKinesisClient
                (TestUtils.getTestAwsCredentialsProvider())
                .withRegion(
                        Regions.fromName(TestConfiguration.get().getTestRegion()));
        for (List<String> partition : partitions) {
            List<PutRecordsRequestEntry> allRecords = newArrayList();
            for (String row : partition) {
                allRecords.add(new PutRecordsRequestEntry().
                        withData(ByteBuffer.wrap(row.getBytes(Charsets.UTF_8))).
                        withPartitionKey(Integer.toString(row.hashCode()))

                );
            }

            PutRecordsResult result;
            do {
                result = client.putRecords(
                        new PutRecordsRequest().
                                withStreamName(TestConfiguration.get().getTestKinesisStream()).
                                withRecords(allRecords));
                List<PutRecordsRequestEntry> failedRecords = newArrayList();
                int i = 0;
                for (PutRecordsResultEntry row : result.getRecords()) {
                    if (row.getErrorCode() != null) {
                        failedRecords.add(allRecords.get(i));
                    }
                    ++i;
                }
                allRecords = failedRecords;
            }

            while (result.getFailedRecordCount() > 0);
        }
    }

    private static class RecordUploaderFuture implements KinesisUploader.RecordUploadFuture {

        private final Future<Void> future;

        public RecordUploaderFuture(Future<Void> future) {
            this.future = future;
        }

        @Override
        public void waitForFinish(long timeout) throws TimeoutException {
            try {
                future.get(timeout, TimeUnit.MILLISECONDS);
            } catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
