package org.apache.beam.sdk.io.kinesis.source.checkpoint;/*
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

import org.apache.beam.sdk.io.kinesis.client.SimplifiedKinesisClient;
import org.apache.beam.sdk.io.kinesis.client.response.KinesisRecord;
import org.apache.beam.sdk.io.kinesis.source.ShardRecordsIterator;

import com.amazonaws.services.kinesis.clientlibrary.types.ExtendedSequenceNumber;
import java.io.IOException;
import java.io.Serializable;

public class ShardCheckpoint implements Serializable {
    private final PositionInShard positionInShard;
    private final String lastKnownShardIterator;

    public ShardCheckpoint(PositionInShard positionInShard, SimplifiedKinesisClient
            kinesisClient) throws IOException {
        this(positionInShard, positionInShard.obtainShardIterator(kinesisClient));
    }

    public ShardCheckpoint(PositionInShard positionInShard, String lastKnownShardIterator) {
        this.positionInShard = positionInShard;
        this.lastKnownShardIterator = lastKnownShardIterator;
    }

    public ShardCheckpoint moveAfter(KinesisRecord record) {
        return new ShardCheckpoint(
                positionInShard.moveAfter(record),
                record.getShardIterator()
        );
    }

    @Override
    public String toString() {
        return String.format("Checkpoint %s with iterator %s", positionInShard,
                lastKnownShardIterator);
    }

    public ShardRecordsIterator getShardRecordsIterator(SimplifiedKinesisClient kinesis)
            throws IOException {
        return new ShardRecordsIterator(this, kinesis);
    }

    public ShardCheckpoint renewShardIterator(SimplifiedKinesisClient client) throws IOException {
        return new ShardCheckpoint(positionInShard, positionInShard.obtainShardIterator(client));
    }

    public String getShardIterator() {
        return lastKnownShardIterator;
    }

    public boolean isBeforeOrAt(ExtendedSequenceNumber extendedSequenceNumber) {
        return positionInShard.isBeforeOrAt(extendedSequenceNumber);
    }
}
