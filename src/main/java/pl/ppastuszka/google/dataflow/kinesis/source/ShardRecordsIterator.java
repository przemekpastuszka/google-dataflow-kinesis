package pl.ppastuszka.google.dataflow.kinesis.source;

import com.amazonaws.services.kinesis.model.GetRecordsRequest;
import com.amazonaws.services.kinesis.model.GetRecordsResult;
import com.amazonaws.services.kinesis.model.Record;
import com.google.cloud.dataflow.sdk.repackaged.com.google.common.base.AbsentWithNoSuchElementException;
import com.google.cloud.dataflow.sdk.repackaged.com.google.common.base.MyOptional;
import com.google.cloud.dataflow.sdk.repackaged.com.google.common.base.Optional;
import com.google.cloud.dataflow.sdk.repackaged.com.google.common.collect.Queues;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pl.ppastuszka.google.dataflow.kinesis.client.provider.KinesisClientProvider;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Iterator;


public class ShardRecordsIterator implements Iterator<Optional<Record>> {
    private static final Logger LOG = LoggerFactory.getLogger(ShardRecordsIterator.class);

    private final KinesisClientProvider kinesis;
    private SingleShardCheckpoint checkpoint;
    private String shardIterator;

    private Deque<Record> data = Queues.newArrayDeque();

    public ShardRecordsIterator(SingleShardCheckpoint checkpoint, KinesisClientProvider kinesisClientProvider) {
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
            return MyOptional.absent();
        } else {
            Record record = data.removeFirst();
            checkpoint = checkpoint.moveAfter(record.getSequenceNumber());
            LOG.debug("Reading record with following sequence number: %s", record.getSequenceNumber());
            return MyOptional.of(record);
        }
    }


    private void readMoreIfNecessary() {
        if (data.isEmpty()) {
            LOG.info("Sending request for more data to Kinesis");

            GetRecordsResult response = kinesis.get().getRecords(shardIterator);
            shardIterator = response.getNextShardIterator();
            data.addAll(response.getRecords());
        }
    }

    public SingleShardCheckpoint getCheckpoint() {
        return checkpoint;
    }
}
