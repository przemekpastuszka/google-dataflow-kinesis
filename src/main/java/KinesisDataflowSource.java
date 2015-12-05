import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.model.Shard;
import com.amazonaws.services.kinesis.model.StreamDescription;
import com.google.cloud.dataflow.sdk.coders.ByteArrayCoder;
import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.SerializableCoder;
import com.google.cloud.dataflow.sdk.io.UnboundedSource;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.repackaged.com.google.common.collect.Lists;
import java.util.List;

import static java.util.Collections.singletonList;

/**
 * Created by ppastuszka on 05.12.15.
 */
public class KinesisDataflowSource extends UnboundedSource<byte[], KinesisCheckpoint> {

    private final AmazonKinesis kinesis;
    private final String streamName;
    private final String shardId;

    public KinesisDataflowSource(AmazonKinesis kinesis, String streamName, String shardId) {
        this.kinesis = kinesis;
        this.streamName = streamName;
        this.shardId = shardId;
    }

    public KinesisDataflowSource(AmazonKinesis kinesis, String streamName) {
        this(kinesis, streamName, null);
    }

    @Override
    public List<KinesisDataflowSource> generateInitialSplits(int desiredNumSplits, PipelineOptions options) throws Exception {
        if (shardId == null) {
            StreamDescription streamDescription = kinesis.describeStream(streamName).getStreamDescription();
            List<Shard> shards = streamDescription.getShards();
            List<KinesisDataflowSource> shardsSources = Lists.newArrayList();
            for (Shard shard : shards) {
                shardsSources.add(new KinesisDataflowSource(kinesis, streamName, shard.getShardId()));
            }
            return shardsSources;
        } else {
            return singletonList(this);
        }
    }

    @Override
    public UnboundedReader<byte[]> createReader(PipelineOptions options, KinesisCheckpoint checkpointMark) {
        return new KinesisReader(kinesis, streamName, shardId, checkpointMark, options, this);
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
