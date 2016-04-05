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
import com.google.common.util.concurrent.ListenableFuture;

import com.amazonaws.services.kinesis.producer.KinesisProducer;
import com.amazonaws.services.kinesis.producer.KinesisProducerConfiguration;
import com.amazonaws.services.kinesis.producer.UserRecordFailedException;
import com.amazonaws.services.kinesis.producer.UserRecordResult;
import static java.lang.System.currentTimeMillis;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import utils.TestConfiguration;
import utils.TestUtils;

/**
 *
 */
public class KinesisProducerLibraryUploader implements RecordsUploader {
    private KinesisProducer producer;

    public KinesisProducerLibraryUploader() {
        producer = new KinesisProducer(
                new KinesisProducerConfiguration().
                        setRateLimit(90).
                        setCredentialsProvider(TestUtils.getTestAwsCredentialsProvider()).
                        setRegion(TestConfiguration.get().getTestRegion())
        );
    }

    @Override
    public RecordUploadFuture startUploadingRecords(List<String> data) {
        List<ListenableFuture<UserRecordResult>> futures = newArrayList();
        for (String s : data) {
            ListenableFuture<UserRecordResult> future = producer.addUserRecord(
                    TestConfiguration.get().getTestKinesisStream(),
                    Integer.toString(s.hashCode()),
                    ByteBuffer.wrap(s.getBytes(Charsets.UTF_8)));
            futures.add(future);
        }
        return new RecordsFutures(futures);
    }

    @Override
    public String toString() {
        return "KinesisProducerLibrary uploader";
    }

    private static class RecordsFutures implements RecordsUploader.RecordUploadFuture {

        private final List<ListenableFuture<UserRecordResult>> futures;

        RecordsFutures(List<ListenableFuture<UserRecordResult>> futures) {
            this.futures = futures;
        }

        @Override
        public void waitForFinish(long timeout) throws TimeoutException {
            long startTime = currentTimeMillis();

            for (ListenableFuture<UserRecordResult> future : futures) {
                try {
                    long timeLeft = verifyTimeLeft(timeout, startTime);
                    UserRecordResult result = future.get(timeLeft, TimeUnit.MILLISECONDS);
                    verifyResult(result);
                } catch (ExecutionException e) {
                    if (e.getCause() instanceof UserRecordFailedException) {
                        verifyResult(((UserRecordFailedException) e.getCause()).getResult());
                    } else {
                        throw new RuntimeException(e);
                    }
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }

        private long verifyTimeLeft(long timeout, long startTime) throws TimeoutException {
            long timeLeft = timeout - (currentTimeMillis() - startTime);
            if (timeLeft < 0) {
                throw new TimeoutException();
            }
            return timeLeft;
        }

        private void verifyResult(UserRecordResult result) {
            if (!result.isSuccessful()) {
                throw new RuntimeException("Failed to send record: " + result.getAttempts().get(0)
                        .getErrorMessage());
            }
        }
    }
}
