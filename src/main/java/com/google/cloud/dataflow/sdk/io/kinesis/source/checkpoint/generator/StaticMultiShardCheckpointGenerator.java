package com.google.cloud.dataflow.sdk.io.kinesis.source.checkpoint.generator;

import static com.google.api.client.repackaged.com.google.common.base.Preconditions.checkNotNull;

import com.google.cloud.dataflow.sdk.io.kinesis.client.SimplifiedKinesisClient;
import com.google.cloud.dataflow.sdk.io.kinesis.source.checkpoint.MultiShardCheckpoint;

/**
 * Created by ppastuszka on 12.12.15.
 */
public class StaticMultiShardCheckpointGenerator implements MultiShardCheckpointGenerator {
    private final MultiShardCheckpoint checkpoint;

    public StaticMultiShardCheckpointGenerator(MultiShardCheckpoint checkpoint) {
        checkNotNull(checkpoint);
        this.checkpoint = checkpoint;
    }

    @Override
    public MultiShardCheckpoint generate(SimplifiedKinesisClient client) {
        return checkpoint;
    }

    @Override
    public String toString() {
        return checkpoint.toString();
    }
}
