package pl.ppastuszka.google.dataflow.kinesis.source;

import com.amazonaws.services.kinesis.model.GetRecordsRequest;
import com.amazonaws.services.kinesis.model.GetRecordsResult;
import com.amazonaws.services.kinesis.model.Record;
import com.google.cloud.dataflow.sdk.repackaged.com.google.common.base.Optional;
import com.google.cloud.dataflow.sdk.repackaged.com.google.common.collect.Queues;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pl.ppastuszka.google.dataflow.kinesis.client.provider.KinesisClientProvider;

import java.util.ArrayDeque;
import java.util.Iterator;


public class ShardRecordsIterator implements Iterator<Optional<Record>> {
    private static final Logger LOG = LoggerFactory.getLogger(ShardRecordsIterator.class);

    private final KinesisClientProvider kinesis;
    private KinesisCheckpoint checkpoint;
    private String shardIterator;

    private ArrayDeque<Record> data = Queues.newArrayDeque();

    public ShardRecordsIterator(KinesisCheckpoint checkpoint, KinesisClientProvider kinesisClientProvider) {
        this.checkpoint = checkpoint;
        this.kinesis = kinesisClientProvider;
        shardIterator = checkpoint.getShardIterator(kinesisClientProvider);
    }

    @Override
    public boolean hasNext() {
        return true;
    }

    @Override
    public Optional<Record> next() {
        readMoreIfNecessary();

        if (data.isEmpty()) {
            return Optional.absent();
        } else {
            Record record = data.removeFirst();
            checkpoint = checkpoint.sameShardAfter(record.getSequenceNumber());
            LOG.debug("Reading record with following sequence number: " + record.getSequenceNumber());
            return Optional.of(record);
        }
    }


    private void readMoreIfNecessary() {
        if (data.isEmpty()) {
            LOG.info("Sending request for more data to Kinesis");

            GetRecordsRequest getRecordsRequest = new GetRecordsRequest();
            GetRecordsResult response = kinesis.getKinesisClient().getRecords(getRecordsRequest.withShardIterator(shardIterator));
            shardIterator = response.getNextShardIterator();
            data.addAll(response.getRecords());
        }
    }

    public KinesisCheckpoint getCheckpoint() {
        return checkpoint;
    }
}
