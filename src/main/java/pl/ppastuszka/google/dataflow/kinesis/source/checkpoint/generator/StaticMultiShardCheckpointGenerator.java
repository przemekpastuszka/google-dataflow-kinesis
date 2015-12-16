package pl.ppastuszka.google.dataflow.kinesis.source.checkpoint.generator;

import pl.ppastuszka.google.dataflow.kinesis.source.checkpoint.MultiShardCheckpoint;

import static com.google.api.client.repackaged.com.google.common.base.Preconditions.checkNotNull;

/**
 * Created by ppastuszka on 12.12.15.
 */
public class StaticMultiShardCheckpointGenerator implements MultiShardCheckpointGenerator {
    private final MultiShardCheckpoint checkpoint;

    public StaticMultiShardCheckpointGenerator(MultiShardCheckpoint checkpoint) {
        checkNotNull(checkpoint);
        this.checkpoint = checkpoint;
    }

    @Override
    public MultiShardCheckpoint generate() {
        return checkpoint;
    }

    @Override
    public String toString() {
        return checkpoint.toString();
    }
}
