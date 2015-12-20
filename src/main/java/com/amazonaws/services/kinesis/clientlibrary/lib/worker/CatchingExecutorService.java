package com.amazonaws.services.kinesis.clientlibrary.lib.worker;

import com.google.cloud.dataflow.sdk.repackaged.com.google.common.util.concurrent
        .ForwardingListeningExecutorService;
import com.google.cloud.dataflow.sdk.repackaged.com.google.common.util.concurrent.ListenableFuture;
import com.google.cloud.dataflow.sdk.repackaged.com.google.common.util.concurrent
        .ListeningExecutorService;
import com.google.cloud.dataflow.sdk.repackaged.com.google.common.util.concurrent.MoreExecutors;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

/**
 * Created by ppastuszka on 20.12.15.
 */
public class CatchingExecutorService extends ForwardingListeningExecutorService {
    private final ListeningExecutorService proxy = MoreExecutors.newDirectExecutorService();
    private ListenableFuture lastFuture;

    public Object waitAndGetLastFuture() throws ExecutionException, InterruptedException {
        return lastFuture.get();
    }

    @Override
    public <T> ListenableFuture<T> submit(Callable<T> task) {
        lastFuture = proxy.submit(task);
        return lastFuture;
    }

    @Override
    protected ListeningExecutorService delegate() {
        return proxy;
    }
}
