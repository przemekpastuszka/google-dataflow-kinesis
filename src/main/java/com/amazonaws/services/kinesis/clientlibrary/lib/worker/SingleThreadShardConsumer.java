package com.amazonaws.services.kinesis.clientlibrary.lib.worker;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.ICheckpoint;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.proxies.IKinesisProxy;
import com.amazonaws.services.kinesis.metrics.impl.NullMetricsFactory;
import com.amazonaws.services.kinesis.model.LimitExceededException;
import com.amazonaws.services.kinesis.model.ProvisionedThroughputExceededException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

/**
 * Created by ppastuszka on 20.12.15.
 */
public class SingleThreadShardConsumer {
    private static final Logger LOG = LoggerFactory.getLogger(SingleThreadShardConsumer.class);
    private final ShardConsumer shardConsumer;
    private final CatchingExecutorService executorService;

    public SingleThreadShardConsumer(String shardId, IKinesisProxy proxy, ICheckpoint checkpoint,
                                     IRecordProcessor recordProcessor) {
        this.executorService = new CatchingExecutorService();
        this.shardConsumer = new ShardConsumer(new ShardInfo(shardId, null, null), new StreamConfig(
                proxy, 10000, 0, true, false, InitialPositionInStream.LATEST
        ), checkpoint, recordProcessor,
                null, 0,
                false, executorService, new NullMetricsFactory(), 0);
    }

    public void consume() throws IOException {
        wrapExceptions(new Callable<Object>() {
            @Override
            public Object call() throws Exception {
                shardConsumer.consumeShard();
                try {
                    Exception exception = ((TaskResult) executorService.waitAndGetLastFuture())
                            .getException();
                    if (exception != null) {
                        throw exception;
                    }
                } catch (ExecutionException e) {
                    throw (Exception) e.getCause();
                }
                return null;
            }
        });

    }

    private <T> T wrapExceptions(Callable<T> callable) throws IOException {
        try {
            return callable.call();
        } catch (LimitExceededException | ProvisionedThroughputExceededException e) {
            LOG.warn("Too many requests to Kinesis", e);
            throw new IOException(e);
        } catch (AmazonServiceException e) {
            if (e.getErrorType() == AmazonServiceException.ErrorType.Service) {
                LOG.warn("Kinesis backend failed", e);
                throw new IOException(e);
            }
            LOG.error("Client side failure", e);
            throw new RuntimeException(e);
        } catch (Exception e) {
            LOG.error("Unknown failure", e);
            throw new RuntimeException(e);
        }
    }
}
