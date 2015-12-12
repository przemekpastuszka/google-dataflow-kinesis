package pl.ppastuszka.google.dataflow.kinesis.source;

import static com.google.api.client.util.Lists.newArrayList;
import static com.google.cloud.dataflow.sdk.repackaged.com.google.common.base.Preconditions
        .checkNotNull;
import static com.google.common.collect.Lists.partition;
import com.google.cloud.dataflow.sdk.coders.ByteArrayCoder;
import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.SerializableCoder;
import com.google.cloud.dataflow.sdk.io.UnboundedSource;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;

import com.amazonaws.services.kinesis.model.ShardIteratorType;
import java.util.List;
import pl.ppastuszka.google.dataflow.kinesis.client.provider.KinesisClientProvider;
import pl.ppastuszka.google.dataflow.kinesis.source.checkpoint.MultiShardCheckpoint;
import pl.ppastuszka.google.dataflow.kinesis.source.checkpoint.SingleShardCheckpoint;
import pl.ppastuszka.google.dataflow.kinesis.source.checkpoint.generator
        .DynamicMultiShardCheckpointGenerator;
import pl.ppastuszka.google.dataflow.kinesis.source.checkpoint.generator
        .MultiShardCheckpointGenerator;
import pl.ppastuszka.google.dataflow.kinesis.source.checkpoint.generator
        .StaticMultiShardCheckpointGenerator;


/***
 *
 */
public class KinesisDataflowSource extends UnboundedSource<byte[], MultiShardCheckpoint> {
    private final KinesisClientProvider kinesis;
    private MultiShardCheckpointGenerator initialCheckpointGenerator;

    public KinesisDataflowSource(KinesisClientProvider kinesis, String streamName,
                                 ShardIteratorType startIteratorType) {
        this(
                kinesis,
                new DynamicMultiShardCheckpointGenerator(kinesis, streamName, startIteratorType));
    }

    public KinesisDataflowSource(KinesisClientProvider kinesisClientProvider,
                                 MultiShardCheckpointGenerator initialCheckpoint) {
        this.kinesis = kinesisClientProvider;
        this.initialCheckpointGenerator = initialCheckpoint;
        validate();
    }

    @Override
    public List<KinesisDataflowSource> generateInitialSplits(
            int desiredNumSplits, PipelineOptions options) throws Exception {
        MultiShardCheckpoint multiShardCheckpoint = initialCheckpointGenerator.generate();

        int partitionSize = Math.max(multiShardCheckpoint.size() / desiredNumSplits, 1);

        List<KinesisDataflowSource> sources = newArrayList();
        for (List<SingleShardCheckpoint> shardPartition :
                partition(multiShardCheckpoint, partitionSize)) {

            MultiShardCheckpoint newCheckpoint = new MultiShardCheckpoint(shardPartition);

            sources.add(
                    new KinesisDataflowSource(
                            kinesis,
                            new StaticMultiShardCheckpointGenerator(newCheckpoint)));
        }
        return sources;
    }

    @Override
    public UnboundedReader<byte[]> createReader(
            PipelineOptions options, MultiShardCheckpoint checkpointMark) {

        MultiShardCheckpointGenerator checkpointGenerator = initialCheckpointGenerator;

        if (checkpointMark != null) {
            checkpointGenerator = new StaticMultiShardCheckpointGenerator(checkpointMark);
        }

        return new KinesisReader(kinesis, checkpointGenerator, options, this);
    }

    @Override
    public Coder<MultiShardCheckpoint> getCheckpointMarkCoder() {
        return SerializableCoder.of(MultiShardCheckpoint.class);
    }

    @Override
    public void validate() {
        checkNotNull(kinesis);
        checkNotNull(initialCheckpointGenerator);
    }

    @Override
    public Coder<byte[]> getDefaultOutputCoder() {
        return ByteArrayCoder.of();
    }


}
