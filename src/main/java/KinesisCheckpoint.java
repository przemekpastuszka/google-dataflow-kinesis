import com.google.cloud.dataflow.sdk.io.UnboundedSource;

import java.io.IOException;
import java.io.Serializable;

/**
 * Created by ppastuszka on 05.12.15.
 */
public class KinesisCheckpoint implements UnboundedSource.CheckpointMark, Serializable {
    private final String shardIteratorType;
    private final String sequenceNumber;

    public KinesisCheckpoint(String shardIteratorType, String sequenceNumber) {
        this.shardIteratorType = shardIteratorType;
        this.sequenceNumber = sequenceNumber;
    }

    @Override
    public void finalizeCheckpoint() throws IOException {

    }

    public String getSequenceNumber() {
        return sequenceNumber;
    }

    public String getShardIteratorType() {
        return shardIteratorType;
    }
}
