package pl.ppastuszka.google.dataflow.kinesis.client;


import com.google.common.collect.Lists;

import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.model.GetRecordsRequest;
import com.amazonaws.services.kinesis.model.GetRecordsResult;
import com.amazonaws.services.kinesis.model.Shard;
import com.amazonaws.services.kinesis.model.ShardIteratorType;
import com.amazonaws.services.kinesis.model.StreamDescription;

import java.util.List;

/***
 *
 */
public class SimplifiedKinesisClient {
    private final AmazonKinesis kinesis;

    public SimplifiedKinesisClient(AmazonKinesis kinesis) {
        this.kinesis = kinesis;
    }

    public AmazonKinesis getRawClient() {
        return kinesis;
    }

    public String getShardIterator(String streamName, String shardId, ShardIteratorType
            shardIteratorType, String startingSequenceNumber) {
        return kinesis.getShardIterator(streamName, shardId, shardIteratorType.toString(),
                startingSequenceNumber).getShardIterator();
    }

    public List<Shard> listShards(String streamName) {
        List<Shard> shards = Lists.newArrayList();
        String lastShardId = null;

        StreamDescription description;
        do {
            description = kinesis.describeStream(streamName, lastShardId).getStreamDescription();
            shards.addAll(description.getShards());
            lastShardId = shards.get(shards.size() - 1).getShardId();
        } while (description.getHasMoreShards());

        return shards;
    }

    public GetRecordsResult getRecords(String shardIterator) {
        return getRecords(shardIterator, null);
    }

    public GetRecordsResult getRecords(String shardIterator, Integer limit) {
        return kinesis.getRecords(new GetRecordsRequest().withShardIterator(shardIterator)
                .withLimit(limit));
    }
}
