package utils;

import utils.kinesis.RecordsUploader;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Created by p.pastuszka on 05/04/16.
 */
public class PubSubUploader implements RecordsUploader {

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

    @Override
    public RecordUploadFuture startUploadingRecords(List<String> data) {
        return new PubSubRecordUploadFuture(PubSubUtil.get().startSendingRecordsToPubSub(data));
    }
}
