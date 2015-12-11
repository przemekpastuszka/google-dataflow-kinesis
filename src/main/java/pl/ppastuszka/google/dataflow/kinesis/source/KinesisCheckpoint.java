package pl.ppastuszka.google.dataflow.kinesis.source;

import com.amazonaws.services.kinesis.model.ShardIteratorType;
import com.google.cloud.dataflow.sdk.io.UnboundedSource;
import pl.ppastuszka.google.dataflow.kinesis.client.provider.KinesisClientProvider;

import java.io.IOException;
import java.io.Serializable;

import static com.amazonaws.services.kinesis.model.ShardIteratorType.AFTER_SEQUENCE_NUMBER;
import static com.amazonaws.services.kinesis.model.ShardIteratorType.AT_SEQUENCE_NUMBER;
import static com.google.api.client.repackaged.com.google.common.base.Preconditions.checkArgument;
import static com.google.api.client.repackaged.com.google.common.base.Preconditions.checkNotNull;

/**
 * Created by ppastuszka on 05.12.15.
 */
public class KinesisCheckpoint implements UnboundedSource.CheckpointMark, Serializable {
    private final String streamName;
    private final String shardId;
    private final String shardIteratorType;
    private final String sequenceNumber;

    public KinesisCheckpoint(String streamName, String shardId, ShardIteratorType shardIteratorType) {
        this(streamName, shardId, shardIteratorType, null);
    }

    public KinesisCheckpoint(String streamName, String shardId, ShardIteratorType shardIteratorType, String sequenceNumber) {
        checkNotNull(streamName);
        checkNotNull(shardId);
        checkNotNull(shardIteratorType);
        if (shardIteratorType == AT_SEQUENCE_NUMBER || shardIteratorType == AFTER_SEQUENCE_NUMBER) {
            checkNotNull(sequenceNumber, "You must provide sequence number for AT_SEQUENCE_NUMBER of AFTER_SEQUENCE_NUMBER");
        } else {
            checkArgument(sequenceNumber == null, "Sequence number must be null for LATEST and TRIM_HORIZON");
        }

        this.streamName = streamName;
        this.shardId = shardId;
        this.shardIteratorType = shardIteratorType.toString();
        this.sequenceNumber = sequenceNumber;
    }

    public KinesisCheckpoint sameShardAfter(String sequenceNumber) {
        return new KinesisCheckpoint(streamName, shardId, AFTER_SEQUENCE_NUMBER, sequenceNumber);
    }

    public String getShardIterator(KinesisClientProvider kinesis) {
        return kinesis.getKinesisClient().getShardIterator(streamName, shardId, shardIteratorType, sequenceNumber)
                .getShardIterator();
    }

    @Override
    public void finalizeCheckpoint() throws IOException {

    }
}
