package com.google.cloud.dataflow.sdk.io.kinesis.source;

import static com.google.api.client.util.Lists.newArrayList;
import static com.google.cloud.dataflow.sdk.repackaged.com.google.common.base.Preconditions
        .checkNotNull;
import static com.google.common.collect.Lists.partition;
import com.google.cloud.dataflow.sdk.coders.ByteArrayCoder;
import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.SerializableCoder;
import com.google.cloud.dataflow.sdk.io.UnboundedSource;
import com.google.cloud.dataflow.sdk.io.kinesis.client.KinesisClientProvider;
import com.google.cloud.dataflow.sdk.io.kinesis.client.SimplifiedKinesisClient;
import com.google.cloud.dataflow.sdk.io.kinesis.source.checkpoint.MultiShardCheckpoint;
import com.google.cloud.dataflow.sdk.io.kinesis.source.checkpoint.SingleShardCheckpoint;
import com.google.cloud.dataflow.sdk.io.kinesis.source.checkpoint.generator
        .DynamicMultiShardCheckpointGenerator;
import com.google.cloud.dataflow.sdk.io.kinesis.source.checkpoint.generator
        .MultiShardCheckpointGenerator;
import com.google.cloud.dataflow.sdk.io.kinesis.source.checkpoint.generator
        .StaticMultiShardCheckpointGenerator;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;

import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.List;


/***
 *
 */
public class KinesisDataflowSource extends UnboundedSource<byte[], MultiShardCheckpoint> {
    private static final Logger LOG = LoggerFactory.getLogger(KinesisDataflowSource.class);

    private final KinesisClientProvider kinesis;
    private MultiShardCheckpointGenerator initialCheckpointGenerator;

    public KinesisDataflowSource(KinesisClientProvider kinesis, String streamName,
                                 InitialPositionInStream initialPositionInStream) {
        this(
                kinesis,
                new DynamicMultiShardCheckpointGenerator(streamName, initialPositionInStream));
    }

    KinesisDataflowSource(KinesisClientProvider kinesisClientProvider,
                          MultiShardCheckpointGenerator initialCheckpoint) {
        this.kinesis = kinesisClientProvider;
        this.initialCheckpointGenerator = initialCheckpoint;
        validate();
    }

    @Override
    public List<KinesisDataflowSource> generateInitialSplits(
            int desiredNumSplits, PipelineOptions options) throws Exception {
        MultiShardCheckpoint multiShardCheckpoint =
                initialCheckpointGenerator.generate(SimplifiedKinesisClient.from(kinesis));
        int partitionSize = Math.max(multiShardCheckpoint.size() / desiredNumSplits, 1);

        List<KinesisDataflowSource> sources = newArrayList();
        List<List<SingleShardCheckpoint>> partitions = partition(multiShardCheckpoint,
                partitionSize);

        LOG.info("Generating {} partitions, each with no more than {} elements",
                partitions.size(), partitionSize);

        for (List<SingleShardCheckpoint> shardPartition :
                partitions) {

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

        LOG.info("Creating new reader using {}", checkpointGenerator);

        return new KinesisReader(SimplifiedKinesisClient.from(kinesis),
                checkpointGenerator,
                options, this);
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
