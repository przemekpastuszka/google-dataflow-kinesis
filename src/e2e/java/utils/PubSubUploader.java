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

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import utils.kinesis.RecordsUploader;

/**
 * Created by p.pastuszka on 05/04/16.
 */
public class PubSubUploader implements RecordsUploader {

    @Override
    public RecordUploadFuture startUploadingRecords(List<String> data) {
        return new PubSubRecordUploadFuture(PubSubUtil.get().startSendingRecordsToPubSub(data));
    }

    private static class PubSubRecordUploadFuture implements RecordsUploader.RecordUploadFuture {

        private final List<Future<?>> futures;

        public PubSubRecordUploadFuture(List<Future<?>> futures) {
            this.futures = futures;
        }

        @Override
        public void waitForFinish(long timeout) throws TimeoutException {
            long endTime = System.currentTimeMillis() + timeout;
            for (Future<?> f : futures) {
                try {
                    f.get(Math.max(0, endTime - System.currentTimeMillis()), TimeUnit.MILLISECONDS);
                } catch (InterruptedException | ExecutionException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }
}
