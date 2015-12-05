import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.model.ShardIteratorType;
import com.google.cloud.dataflow.sdk.io.UnboundedSource;

import java.io.IOException;
import java.io.Serializable;

import static com.google.api.client.repackaged.com.google.common.base.Preconditions.checkNotNull;

/**
 * Created by ppastuszka on 05.12.15.
 */
public class KinesisCheckpoint implements UnboundedSource.CheckpointMark, Serializable {
    private final String shardIteratorType;
    private final String sequenceNumber;

    public KinesisCheckpoint(ShardIteratorType shardIteratorType) {
        this(shardIteratorType, null);
    }

    public KinesisCheckpoint(ShardIteratorType shardIteratorType, String sequenceNumber) {
        checkNotNull(shardIteratorType);

        this.shardIteratorType = shardIteratorType.toString();
        this.sequenceNumber = sequenceNumber;
    }

    public String getShardIterator(AmazonKinesis kinesis, String streamName, String shardId) {
        return kinesis.getShardIterator(streamName, shardId, shardIteratorType, sequenceNumber).getShardIterator();
    }

    @Override
    public void finalizeCheckpoint() throws IOException {

    }
}
