package pl.ppastuszka.google.dataflow.kinesis.client.provider;

import java.io.Serializable;
import pl.ppastuszka.google.dataflow.kinesis.client.SimplifiedKinesisClient;

/**
 * Created by ppastuszka on 05.12.15.
 */
public interface KinesisClientProvider extends Serializable {
    SimplifiedKinesisClient get();
}
