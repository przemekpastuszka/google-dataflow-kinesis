package com.google.cloud.dataflow.sdk.io.kinesis.source.checkpoint.generator;

import static com.google.api.client.repackaged.com.google.common.base.Preconditions.checkNotNull;

import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.amazonaws.services.kinesis.model.Shard;
import java.io.IOException;

import com.google.cloud.dataflow.sdk.io.kinesis.client.SimplifiedKinesisClient;
import com.google.cloud.dataflow.sdk.io.kinesis.source.checkpoint.MultiShardCheckpoint;
import com.google.cloud.dataflow.sdk.io.kinesis.source.checkpoint.SingleShardCheckpoint;

/**
 * Created by ppastuszka on 12.12.15.
 */
public class DynamicMultiShardCheckpointGenerator implements MultiShardCheckpointGenerator {
    private final String streamName;
    private final InitialPositionInStream startPosition;

    public DynamicMultiShardCheckpointGenerator(String streamName,
                                                InitialPositionInStream startPosition) {
        checkNotNull(streamName);
        checkNotNull(startPosition);

        this.streamName = streamName;
        this.startPosition = startPosition;
    }

    @Override
    public MultiShardCheckpoint generate(SimplifiedKinesisClient kinesis) throws IOException {
        MultiShardCheckpoint checkpoint = new MultiShardCheckpoint();
        for (Shard shard : kinesis.listShards(streamName)) {
            checkpoint.add(
                    new SingleShardCheckpoint(streamName, shard.getShardId(), startPosition)
            );
        }
        return checkpoint;
    }

    @Override
    public String toString() {
        return String.format("Checkpoint generator for %s: %s", streamName, startPosition);
    }
}
