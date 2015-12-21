package pl.ppastuszka.google.dataflow.kinesis.source.checkpoint;

import static com.google.cloud.dataflow.sdk.repackaged.com.google.common.base.Preconditions
        .checkNotNull;
import com.google.cloud.dataflow.sdk.io.UnboundedSource;

import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.amazonaws.services.kinesis.clientlibrary.types.ExtendedSequenceNumber;
import java.io.IOException;
import java.io.Serializable;
import pl.ppastuszka.google.dataflow.kinesis.client.SerializableKinesisProxyFactory;
import pl.ppastuszka.google.dataflow.kinesis.source.ShardRecordsIterator;

/***
 *
 */
public class SingleShardCheckpoint implements UnboundedSource.CheckpointMark, Serializable {
    private final String streamName;
    private final String shardId;
    private final String sequenceNumber;
    private final long subSequenceNumber;

    public SingleShardCheckpoint(String streamName, String shardId, InitialPositionInStream
            initialPositionInStream) {

        this(streamName, shardId, initialPositionInStream.toString(), 0L);
    }

    public SingleShardCheckpoint(String streamName, String shardId,
                                 String sequenceNumber, long subSequenceNumber) {
        this.subSequenceNumber = subSequenceNumber;

        checkNotNull(streamName);
        checkNotNull(shardId);
        checkNotNull(sequenceNumber);

        this.streamName = streamName;
        this.shardId = shardId;
        this.sequenceNumber = sequenceNumber;
    }

    public String getStreamName() {
        return streamName;
    }

    public String getShardId() {
        return shardId;
    }

    @Override
    public String toString() {
        return String.format("Checkpoint for stream %s, shard %s: %s", streamName, shardId,
                sequenceNumber);
    }

    public ShardRecordsIterator getShardRecordsIterator(SerializableKinesisProxyFactory kinesis)
            throws IOException {
        return new ShardRecordsIterator(this, kinesis);
    }

    @Override
    public void finalizeCheckpoint() throws IOException {

    }

    public ExtendedSequenceNumber getExtendedSequenceNumber() {
        return new ExtendedSequenceNumber(sequenceNumber, subSequenceNumber);
    }

    public SingleShardCheckpoint moveAfter(ExtendedSequenceNumber checkpointValue) {
        return new SingleShardCheckpoint(
                streamName, shardId,
                checkpointValue.getSequenceNumber(),
                checkpointValue.getSubSequenceNumber());

    }
}
