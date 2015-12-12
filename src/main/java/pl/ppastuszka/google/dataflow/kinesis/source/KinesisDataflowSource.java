package pl.ppastuszka.google.dataflow.kinesis.source;

import com.amazonaws.services.kinesis.model.Shard;
import com.amazonaws.services.kinesis.model.ShardIteratorType;
import com.google.cloud.dataflow.sdk.coders.ByteArrayCoder;
import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.SerializableCoder;
import com.google.cloud.dataflow.sdk.io.UnboundedSource;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import pl.ppastuszka.google.dataflow.kinesis.client.provider.KinesisClientProvider;

import java.util.List;

import static com.google.api.client.util.Lists.newArrayList;
import static com.google.cloud.dataflow.sdk.repackaged.com.google.common.base.Preconditions.checkArgument;
import static com.google.cloud.dataflow.sdk.repackaged.com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Lists.partition;
import static java.lang.Math.max;


public class KinesisDataflowSource extends UnboundedSource<byte[], MultiShardCheckpoint> {
    private final KinesisClientProvider kinesis;
    private MultiShardCheckpoint initialCheckpoint;

    public KinesisDataflowSource(KinesisClientProvider kinesis, String streamName, ShardIteratorType startIteratorType) {
        this(kinesis, calculateInitialCheckpoints(kinesis, streamName, startIteratorType));
    }

    public KinesisDataflowSource(KinesisClientProvider kinesisClientProvider, MultiShardCheckpoint initialCheckpoint) {
        this.kinesis = kinesisClientProvider;
        this.initialCheckpoint = initialCheckpoint;
        validate();
    }

    @Override
    public List<KinesisDataflowSource> generateInitialSplits(int desiredNumSplits, PipelineOptions options) throws Exception {
        int partitionSize = max(initialCheckpoint.size() / desiredNumSplits, 1);

        List<KinesisDataflowSource> sources = newArrayList();
        for (List<SingleShardCheckpoint> singleShardCheckpoint : partition(initialCheckpoint, partitionSize)) {
            sources.add(new KinesisDataflowSource(kinesis, new MultiShardCheckpoint(singleShardCheckpoint)));
        }
        return sources;
    }

    @Override
    public UnboundedReader<byte[]> createReader(PipelineOptions options, MultiShardCheckpoint checkpointMark) {
        if (checkpointMark == null) {
            checkpointMark = initialCheckpoint;
        }

        return new KinesisReader(kinesis, checkpointMark, options, this);
    }

    private static MultiShardCheckpoint calculateInitialCheckpoints(KinesisClientProvider kinesis, String streamName, ShardIteratorType startIteratorType) {
        MultiShardCheckpoint checkpoint = new MultiShardCheckpoint();
        for (Shard shard : kinesis.get().listShards(streamName)) {
            checkpoint.add(new SingleShardCheckpoint(streamName, shard.getShardId(), startIteratorType));
        }
        return checkpoint;
    }

    @Override
    public Coder<MultiShardCheckpoint> getCheckpointMarkCoder() {
        return SerializableCoder.of(MultiShardCheckpoint.class);
    }

    @Override
    public void validate() {
        checkNotNull(kinesis);
        checkNotNull(initialCheckpoint);
        checkArgument(!initialCheckpoint.isEmpty());
    }

    @Override
    public Coder<byte[]> getDefaultOutputCoder() {
        return ByteArrayCoder.of();
    }


}
