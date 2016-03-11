package com.google.cloud.dataflow.sdk.io.kinesis.source.checkpoint;

import static com.google.cloud.dataflow.sdk.repackaged.com.google.common.base.Preconditions
        .checkArgument;
import static com.google.cloud.dataflow.sdk.repackaged.com.google.common.base.Preconditions
        .checkNotNull;
import com.google.cloud.dataflow.sdk.io.UnboundedSource;
import com.google.cloud.dataflow.sdk.io.kinesis.client.SimplifiedKinesisClient;
import com.google.cloud.dataflow.sdk.io.kinesis.source.ShardRecordsIterator;

import static com.amazonaws.services.kinesis.model.ShardIteratorType.AFTER_SEQUENCE_NUMBER;
import static com.amazonaws.services.kinesis.model.ShardIteratorType.AT_SEQUENCE_NUMBER;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.amazonaws.services.kinesis.clientlibrary.types.ExtendedSequenceNumber;
import com.amazonaws.services.kinesis.clientlibrary.types.UserRecord;
import com.amazonaws.services.kinesis.model.Record;
import com.amazonaws.services.kinesis.model.ShardIteratorType;
import java.io.IOException;
import java.io.Serializable;

/***
 *
 */
public class SingleShardCheckpoint implements UnboundedSource.CheckpointMark, Serializable {
    private final String streamName;
    private final String shardId;
    private final String sequenceNumber;
    private final ShardIteratorType shardIteratorType;
    private final long subSequenceNumber;

    public SingleShardCheckpoint(String streamName, String shardId, InitialPositionInStream
            initialPositionInStream) {

        this(streamName, shardId, ShardIteratorType.fromValue(initialPositionInStream.name()),
                null);
    }

    public SingleShardCheckpoint(String streamName, String shardId, ShardIteratorType
            shardIteratorType, String sequenceNumber) {
        this(streamName, shardId, shardIteratorType, sequenceNumber, 0L);
    }

    public SingleShardCheckpoint(String streamName, String shardId, ShardIteratorType
            shardIteratorType, String sequenceNumber, long subSequenceNumber) {

        checkNotNull(streamName);
        checkNotNull(shardId);
        checkNotNull(shardIteratorType);
        if (shardIteratorType == AT_SEQUENCE_NUMBER || shardIteratorType == AFTER_SEQUENCE_NUMBER) {
            checkNotNull(sequenceNumber, "You must provide sequence number for AT_SEQUENCE_NUMBER" +
                    " of AFTER_SEQUENCE_NUMBER");
        } else {
            checkArgument(sequenceNumber == null,
                    "Sequence number must be null for LATEST and TRIM_HORIZON");
        }

        this.subSequenceNumber = subSequenceNumber;
        this.shardIteratorType = shardIteratorType;
        this.streamName = streamName;
        this.shardId = shardId;
        this.sequenceNumber = sequenceNumber;
    }

    public boolean isBefore(ExtendedSequenceNumber other) {
        return extendedSequenceNumber().compareTo(other) < 0;
    }

    private ExtendedSequenceNumber extendedSequenceNumber() {
        String fullSequenceNumber = sequenceNumber;
        if (fullSequenceNumber == null) {
            fullSequenceNumber = shardIteratorType.toString();
        }
        return new ExtendedSequenceNumber(fullSequenceNumber, subSequenceNumber);
    }

    @Override
    public String toString() {
        return String.format("Checkpoint for stream %s, shard %s: %s", streamName, shardId,
                sequenceNumber);
    }

    public ShardRecordsIterator getShardRecordsIterator(SimplifiedKinesisClient kinesis)
            throws IOException {
        return new ShardRecordsIterator(this, kinesis);
    }

    @Override
    public void finalizeCheckpoint() throws IOException {

    }

    public String getShardIterator(SimplifiedKinesisClient kinesisClient) throws IOException {
        return kinesisClient.getShardIterator(streamName,
                shardId, shardIteratorType,
                sequenceNumber);
    }

    public SingleShardCheckpoint moveAfter(Record record) {
        if (record instanceof UserRecord) {
            return new SingleShardCheckpoint(
                    streamName, shardId,
                    AFTER_SEQUENCE_NUMBER,
                    record.getSequenceNumber(),
                    ((UserRecord) record).getSubSequenceNumber());
        }
        return new SingleShardCheckpoint(streamName, shardId,
                AFTER_SEQUENCE_NUMBER, record.getSequenceNumber());
    }
}
