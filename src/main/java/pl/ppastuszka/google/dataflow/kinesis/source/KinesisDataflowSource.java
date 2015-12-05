package pl.ppastuszka.google.dataflow.kinesis.source;

import com.amazonaws.services.kinesis.model.Shard;
import com.amazonaws.services.kinesis.model.ShardIteratorType;
import com.amazonaws.services.kinesis.model.StreamDescription;
import com.google.cloud.dataflow.sdk.coders.ByteArrayCoder;
import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.SerializableCoder;
import com.google.cloud.dataflow.sdk.io.UnboundedSource;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.repackaged.com.google.common.collect.Lists;
import pl.ppastuszka.google.dataflow.kinesis.client.KinesisClientProvider;

import java.util.List;

import static java.util.Collections.singletonList;

/**
 * Created by ppastuszka on 05.12.15.
 */
public class KinesisDataflowSource extends UnboundedSource<byte[], KinesisCheckpoint> {
    private final KinesisClientProvider kinesis;
    private final String streamName;
    private final String shardId;
    private final ShardIteratorType startIteratorType;

    public KinesisDataflowSource(KinesisClientProvider kinesis, String streamName, ShardIteratorType startIteratorType, String shardId) {
        this.kinesis = kinesis;
        this.streamName = streamName;
        this.shardId = shardId;
        this.startIteratorType = startIteratorType;
    }

    public KinesisDataflowSource(KinesisClientProvider kinesis, String streamName, ShardIteratorType startIteratorType) {
        this(kinesis, streamName, startIteratorType, null);
    }

    @Override
    public List<KinesisDataflowSource> generateInitialSplits(int desiredNumSplits, PipelineOptions options) throws Exception {
        if (shardId == null) {
            StreamDescription streamDescription = kinesis.getKinesisClient().describeStream(streamName).getStreamDescription();
            List<Shard> shards = streamDescription.getShards();
            List<KinesisDataflowSource> shardsSources = Lists.newArrayList();
            for (Shard shard : shards) {
                shardsSources.add(new KinesisDataflowSource(kinesis, streamName, startIteratorType, shard.getShardId()));
            }
            return shardsSources;
        } else {
            return singletonList(this);
        }
    }

    @Override
    public UnboundedReader<byte[]> createReader(PipelineOptions options, KinesisCheckpoint checkpointMark) {
        if (checkpointMark == null) {
            checkpointMark = new KinesisCheckpoint(startIteratorType);
        }
        String temporaryShardId = shardId;
        if (temporaryShardId == null) {
            temporaryShardId = "shardId-000000000001";
        }
        return new KinesisReader(kinesis, streamName, temporaryShardId, checkpointMark, options, this);
    }

    @Override
    public Coder<KinesisCheckpoint> getCheckpointMarkCoder() {
        return SerializableCoder.of(KinesisCheckpoint.class);
    }

    @Override
    public void validate() {
        //TODO: implement this
    }

    @Override
    public Coder<byte[]> getDefaultOutputCoder() {
        return ByteArrayCoder.of();
    }


}
