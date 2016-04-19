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
package org.apache.beam.sdk.io.kinesis.source.checkpoint.generator;

import org.apache.beam.sdk.io.kinesis.client.SimplifiedKinesisClient;
import org.apache.beam.sdk.io.kinesis.source.checkpoint.KinesisReaderCheckpoint;
import org.apache.beam.sdk.io.kinesis.source.checkpoint.PositionInShard;
import org.apache.beam.sdk.io.kinesis.source.checkpoint.ShardCheckpoint;
import static com.google.api.client.repackaged.com.google.common.base.Preconditions.checkNotNull;
import com.google.api.client.util.Lists;

import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.amazonaws.services.kinesis.model.Shard;
import java.io.IOException;
import java.util.List;

/**
 * Creates {@link KinesisReaderCheckpoint}, which spans over all shards in given stream.
 * List of shards is obtained dynamically on call to {@link #generate(SimplifiedKinesisClient)}.
 */
public class DynamicCheckpointGenerator implements CheckpointGenerator {
    private final String streamName;
    private final InitialPositionInStream startPosition;

    public DynamicCheckpointGenerator(String streamName, InitialPositionInStream startPosition) {
        checkNotNull(streamName);
        checkNotNull(startPosition);

        this.streamName = streamName;
        this.startPosition = startPosition;
    }

    @Override
    public KinesisReaderCheckpoint generate(final SimplifiedKinesisClient kinesis) throws
            IOException {
        List<ShardCheckpoint> shardCheckpoints = Lists.newArrayList();

        for (Shard shard : kinesis.listShards(streamName)) {
            ShardCheckpoint checkpoint = new ShardCheckpoint(
                    new PositionInShard(streamName, shard.getShardId(), startPosition), kinesis);
            shardCheckpoints.add(checkpoint);
        }

        return new KinesisReaderCheckpoint(shardCheckpoints);
    }

    @Override
    public String toString() {
        return String.format("Checkpoint generator for %s: %s", streamName, startPosition);
    }
}
