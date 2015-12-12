package pl.ppastuszka.google.dataflow.kinesis.source.checkpoint.generator;

import java.io.Serializable;
import pl.ppastuszka.google.dataflow.kinesis.source.checkpoint.MultiShardCheckpoint;

/**
 * Created by ppastuszka on 12.12.15.
 */
public interface MultiShardCheckpointGenerator extends Serializable {
    MultiShardCheckpoint generate();
}
