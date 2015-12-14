package pl.ppastuszka.google.dataflow.kinesis.client;


import com.google.common.collect.Lists;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.model.ExpiredIteratorException;
import com.amazonaws.services.kinesis.model.GetRecordsRequest;
import com.amazonaws.services.kinesis.model.GetRecordsResult;
import com.amazonaws.services.kinesis.model.Shard;
import com.amazonaws.services.kinesis.model.ShardIteratorType;
import com.amazonaws.services.kinesis.model.StreamDescription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.Callable;

/***
 *
 */
public class SimplifiedKinesisClient {
    private static final Logger LOG = LoggerFactory.getLogger(SimplifiedKinesisClient.class);

    private final AmazonKinesis kinesis;

    public SimplifiedKinesisClient(AmazonKinesis kinesis) {
        this.kinesis = kinesis;
    }

    public String getShardIterator(final String streamName, final String shardId,
                                   final ShardIteratorType shardIteratorType,
                                   final String startingSequenceNumber) throws IOException {
        return wrapExceptions(new Callable<String>() {
            @Override
            public String call() throws Exception {
                return kinesis.getShardIterator(
                        streamName, shardId, shardIteratorType.toString(), startingSequenceNumber)
                        .getShardIterator();
            }
        });
    }

    public List<Shard> listShards(final String streamName) throws IOException {
        return wrapExceptions(new Callable<List<Shard>>() {
            @Override
            public List<Shard> call() throws Exception {
                List<Shard> shards = Lists.newArrayList();
                String lastShardId = null;

                StreamDescription description;
                do {
                    description = kinesis.describeStream(streamName, lastShardId)
                            .getStreamDescription();

                    shards.addAll(description.getShards());
                    lastShardId = shards.get(shards.size() - 1).getShardId();
                } while (description.getHasMoreShards());

                return shards;
            }
        });
    }

    public GetRecordsResult getRecords(String shardIterator) throws IOException {
        return getRecords(shardIterator, null);
    }

    public GetRecordsResult getRecords(final String shardIterator, final Integer limit) throws
            IOException {
        return wrapExceptions(new Callable<GetRecordsResult>() {
            @Override
            public GetRecordsResult call() throws Exception {
                return kinesis.getRecords(new GetRecordsRequest()
                        .withShardIterator(shardIterator)
                        .withLimit(limit));
            }
        });
    }

    private <T> T wrapExceptions(Callable<T> callable) throws IOException {
        try {
            return callable.call();
        } catch (ExpiredIteratorException e) {
            throw e;
        } catch (AmazonServiceException e) {
            LOG.error("Call to Amazon Service failed", e);
            throw new IOException(e);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
