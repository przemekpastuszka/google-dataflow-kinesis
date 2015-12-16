package pl.ppastuszka.google.dataflow.kinesis.source.checkpoint.generator;

import static com.google.api.client.repackaged.com.google.common.base.Preconditions.checkArgument;
import static com.google.api.client.repackaged.com.google.common.base.Preconditions.checkNotNull;

import static com.amazonaws.services.kinesis.model.ShardIteratorType.LATEST;
import static com.amazonaws.services.kinesis.model.ShardIteratorType.TRIM_HORIZON;
import com.amazonaws.services.kinesis.model.Shard;
import com.amazonaws.services.kinesis.model.ShardIteratorType;
import java.io.IOException;
import pl.ppastuszka.google.dataflow.kinesis.client.provider.KinesisClientProvider;
import pl.ppastuszka.google.dataflow.kinesis.source.checkpoint.MultiShardCheckpoint;
import pl.ppastuszka.google.dataflow.kinesis.source.checkpoint.SingleShardCheckpoint;

/**
 * Created by ppastuszka on 12.12.15.
 */
public class DynamicMultiShardCheckpointGenerator implements MultiShardCheckpointGenerator {
    private final KinesisClientProvider kinesis;
    private final String streamName;
    private final ShardIteratorType startIteratorType;

    public DynamicMultiShardCheckpointGenerator(KinesisClientProvider kinesis, String streamName,
                                                ShardIteratorType startIteratorType) {
        checkNotNull(kinesis);
        checkNotNull(streamName);
        checkNotNull(startIteratorType);
        checkArgument(startIteratorType == LATEST || startIteratorType == TRIM_HORIZON);

        this.kinesis = kinesis;
        this.streamName = streamName;
        this.startIteratorType = startIteratorType;
    }

    @Override
    public MultiShardCheckpoint generate() throws IOException {
        MultiShardCheckpoint checkpoint = new MultiShardCheckpoint();
        for (Shard shard : kinesis.get().listShards(streamName)) {
            checkpoint.add(
                    new SingleShardCheckpoint(streamName, shard.getShardId(), startIteratorType)
            );
        }
        return checkpoint;
    }

    @Override
    public String toString() {
        return String.format("Checkpoint generator for %s: %s", streamName, startIteratorType);
    }
}
