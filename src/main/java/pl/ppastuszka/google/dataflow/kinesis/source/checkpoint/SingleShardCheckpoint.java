package pl.ppastuszka.google.dataflow.kinesis.source.checkpoint;

import static com.google.cloud.dataflow.sdk.repackaged.com.google.common.base.Preconditions
        .checkArgument;
import static com.google.cloud.dataflow.sdk.repackaged.com.google.common.base.Preconditions
        .checkNotNull;
import com.google.cloud.dataflow.sdk.io.UnboundedSource;

import static com.amazonaws.services.kinesis.model.ShardIteratorType.AFTER_SEQUENCE_NUMBER;
import static com.amazonaws.services.kinesis.model.ShardIteratorType.AT_SEQUENCE_NUMBER;
import com.amazonaws.services.kinesis.model.ShardIteratorType;
import java.io.IOException;
import java.io.Serializable;
import pl.ppastuszka.google.dataflow.kinesis.client.provider.KinesisClientProvider;
import pl.ppastuszka.google.dataflow.kinesis.source.ShardRecordsIterator;

/***
 *
 */
public class SingleShardCheckpoint implements UnboundedSource.CheckpointMark, Serializable {
    private final String streamName;
    private final String shardId;
    private final ShardIteratorType shardIteratorType;
    private final String sequenceNumber;

    public SingleShardCheckpoint(String streamName, String shardId, ShardIteratorType
            shardIteratorType) {

        this(streamName, shardId, shardIteratorType, null);
    }

    public SingleShardCheckpoint(String streamName, String shardId, ShardIteratorType
            shardIteratorType, String sequenceNumber) {

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

        this.streamName = streamName;
        this.shardId = shardId;
        this.shardIteratorType = shardIteratorType;
        this.sequenceNumber = sequenceNumber;
    }

    public SingleShardCheckpoint moveAfter(String sequenceNumber) {
        return new SingleShardCheckpoint(
                streamName, shardId, AFTER_SEQUENCE_NUMBER, sequenceNumber);
    }

    public ShardRecordsIterator getShardRecordsIterator(KinesisClientProvider kinesis) throws
            IOException {
        return new ShardRecordsIterator(this, kinesis);
    }

    public String getShardIterator(KinesisClientProvider kinesis) throws IOException {
        return kinesis.get().
                getShardIterator(streamName, shardId, shardIteratorType, sequenceNumber);
    }

    @Override
    public void finalizeCheckpoint() throws IOException {

    }
}
