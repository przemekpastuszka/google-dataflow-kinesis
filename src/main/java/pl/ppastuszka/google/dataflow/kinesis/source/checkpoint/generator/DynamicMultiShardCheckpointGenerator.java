package pl.ppastuszka.google.dataflow.kinesis.source.checkpoint.generator;

import static com.google.api.client.repackaged.com.google.common.base.Preconditions.checkNotNull;

import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.amazonaws.services.kinesis.model.Shard;
import java.io.IOException;
import pl.ppastuszka.google.dataflow.kinesis.client.SerializableKinesisProxyFactory;
import pl.ppastuszka.google.dataflow.kinesis.source.checkpoint.MultiShardCheckpoint;
import pl.ppastuszka.google.dataflow.kinesis.source.checkpoint.SingleShardCheckpoint;

/**
 * Created by ppastuszka on 12.12.15.
 */
public class DynamicMultiShardCheckpointGenerator implements MultiShardCheckpointGenerator {
    private final SerializableKinesisProxyFactory kinesis;
    private final String streamName;
    private final InitialPositionInStream startPosition;

    public DynamicMultiShardCheckpointGenerator(SerializableKinesisProxyFactory kinesis, String
            streamName,
                                                InitialPositionInStream startPosition) {
        checkNotNull(kinesis);
        checkNotNull(streamName);
        checkNotNull(startPosition);

        this.kinesis = kinesis;
        this.streamName = streamName;
        this.startPosition = startPosition;
    }

    @Override
    public MultiShardCheckpoint generate() throws IOException {
        MultiShardCheckpoint checkpoint = new MultiShardCheckpoint();
        for (Shard shard : kinesis.getProxy(streamName).getShardList()) {
            checkpoint.add(
                    new SingleShardCheckpoint(streamName, shard.getShardId(), startPosition)
            );
        }
        return checkpoint;
    }

    @Override
    public String toString() {
        return String.format("Checkpoint generator for %s: %s", streamName, startPosition);
    }
}
