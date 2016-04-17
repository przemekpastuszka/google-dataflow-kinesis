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
package com.google.cloud.dataflow.sdk.io.kinesis.source;

import static com.google.cloud.dataflow.sdk.repackaged.com.google.common.base.Preconditions
        .checkNotNull;
import static com.google.cloud.dataflow.sdk.repackaged.com.google.common.collect.Lists.newArrayList;
import com.google.cloud.dataflow.sdk.io.UnboundedSource;
import com.google.cloud.dataflow.sdk.io.kinesis.client.SimplifiedKinesisClient;
import com.google.cloud.dataflow.sdk.io.kinesis.client.response.KinesisRecord;
import com.google.cloud.dataflow.sdk.io.kinesis.source.checkpoint.KinesisReaderCheckpoint;
import com.google.cloud.dataflow.sdk.io.kinesis.source.checkpoint.ShardCheckpoint;
import com.google.cloud.dataflow.sdk.io.kinesis.source.checkpoint.generator.CheckpointGenerator;
import com.google.cloud.dataflow.sdk.io.kinesis.utils.RoundRobin;
import com.google.cloud.dataflow.sdk.repackaged.com.google.common.base.CustomOptional;
import com.google.cloud.dataflow.sdk.repackaged.com.google.common.base.Optional;

import com.amazonaws.services.kinesis.model.Record;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.util.List;
import java.util.NoSuchElementException;


/***
 * Reads data from multiple kinesis shards in a single thread.
 */
class KinesisReader extends UnboundedSource.UnboundedReader<Record> {
    private static final Logger LOG = LoggerFactory.getLogger(KinesisReader.class);

    private final SimplifiedKinesisClient kinesis;
    private final UnboundedSource<Record, ?> source;
    private final CheckpointGenerator initialCheckpointGenerator;
    private RoundRobin<ShardRecordsIterator> shardIterators;
    private Optional<KinesisRecord> currentRecord = CustomOptional.absent();
    private Optional<Instant> currendRecordTimestamp = CustomOptional.absent();

    public KinesisReader(SimplifiedKinesisClient kinesis,
                         CheckpointGenerator initialCheckpointGenerator,
                         UnboundedSource<Record, ?> source) {
        checkNotNull(kinesis);
        checkNotNull(initialCheckpointGenerator);

        this.kinesis = kinesis;
        this.source = source;
        this.initialCheckpointGenerator = initialCheckpointGenerator;
    }

    /***
     * Generates initial checkpoint and instantiates iterators for shards.
     */
    @Override
    public boolean start() throws IOException {
        LOG.info("Starting reader using {}", initialCheckpointGenerator);

        KinesisReaderCheckpoint initialCheckpoint = initialCheckpointGenerator.generate(kinesis);
        List<ShardRecordsIterator> iterators = newArrayList();
        for (ShardCheckpoint checkpoint : initialCheckpoint) {
            iterators.add(checkpoint.getShardRecordsIterator(kinesis));
        }
        shardIterators = new RoundRobin<>(iterators);

        return advance();
    }

    /***
     * Moves to the next record in one of the shards.
     * If current shard iterator can be move forward (i.e. there's a record present) then we do it.
     * If not, we iterate over shards in a round-robin manner.
     */
    @Override
    public boolean advance() throws IOException {
        for (int i = 0; i < shardIterators.size(); ++i) {
            currentRecord = shardIterators.getCurrent().next();
            if (currentRecord.isPresent()) {
                currendRecordTimestamp = Optional.of(Instant.now());
                return true;
            } else {
                shardIterators.moveForward();
            }
        }
        return false;
    }

    @Override
    public byte[] getCurrentRecordId() throws NoSuchElementException {
        return currentRecord.get().getUniqueId();
    }

    @Override
    public Record getCurrent() throws NoSuchElementException {
        return currentRecord.get();
    }

    /***
     * When {@link KinesisReader} was advanced to the current record.
     * We cannot use approximate arrival timestamp given for each record by Kinesis as it
     * is not guaranteed to be accurate - this could lead to mark some records as "late"
     * even if they were not.
     */
    @Override
    public Instant getCurrentTimestamp() throws NoSuchElementException {
        return currendRecordTimestamp.get();
    }

    @Override
    public void close() throws IOException {
    }

    /***
     * Current time.
     * We cannot give better approximation of the watermark with current semantics of
     * {@link KinesisReader#getCurrentTimestamp()}, because we don't know when the next
     * {@link KinesisReader#advance()} will be called.
     */
    @Override
    public Instant getWatermark() {
        return Instant.now();
    }

    @Override
    public UnboundedSource.CheckpointMark getCheckpointMark() {
        return KinesisReaderCheckpoint.asCurrentStateOf(shardIterators);
    }

    @Override
    public UnboundedSource<Record, ?> getCurrentSource() {
        return source;
    }

}
