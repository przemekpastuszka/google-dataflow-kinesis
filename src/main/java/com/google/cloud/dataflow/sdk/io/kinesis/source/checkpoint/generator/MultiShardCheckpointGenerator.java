package com.google.cloud.dataflow.sdk.io.kinesis.source.checkpoint.generator;

import com.google.cloud.dataflow.sdk.io.kinesis.client.SimplifiedKinesisClient;
import com.google.cloud.dataflow.sdk.io.kinesis.source.checkpoint.MultiShardCheckpoint;

import java.io.IOException;
import java.io.Serializable;

/**
 * Created by ppastuszka on 12.12.15.
 */
public interface MultiShardCheckpointGenerator extends Serializable {
    MultiShardCheckpoint generate(SimplifiedKinesisClient client) throws IOException;
}
