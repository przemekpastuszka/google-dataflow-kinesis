package pl.ppastuszka.google.dataflow.kinesis.source;

import com.amazonaws.services.kinesis.model.ExpiredIteratorException;
import com.amazonaws.services.kinesis.model.GetRecordsResult;
import com.amazonaws.services.kinesis.model.Record;
import com.google.cloud.dataflow.sdk.repackaged.com.google.common.base.MyOptional;
import com.google.cloud.dataflow.sdk.repackaged.com.google.common.base.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pl.ppastuszka.google.dataflow.kinesis.client.provider.KinesisClientProvider;
import pl.ppastuszka.google.dataflow.kinesis.source.checkpoint.SingleShardCheckpoint;

import java.io.IOException;
import java.util.Deque;

import static com.google.cloud.dataflow.sdk.repackaged.com.google.common.base.Preconditions.checkNotNull;
import static com.google.cloud.dataflow.sdk.repackaged.com.google.common.collect.Queues.newArrayDeque;

/***
 *
 */
public class ShardRecordsIterator {
    private static final Logger LOG = LoggerFactory.getLogger(ShardRecordsIterator.class);

    private final KinesisClientProvider kinesis;
    private SingleShardCheckpoint checkpoint;
    private String shardIterator;

    private Deque<Record> data = newArrayDeque();

    public ShardRecordsIterator(SingleShardCheckpoint checkpoint, KinesisClientProvider
            kinesisClientProvider) throws IOException {
        checkNotNull(checkpoint);
        checkNotNull(kinesisClientProvider);
        this.checkpoint = checkpoint;
        this.kinesis = kinesisClientProvider;
        shardIterator = checkpoint.getShardIterator(kinesisClientProvider);
    }

    public Optional<Record> next() throws IOException {
        readMoreIfNecessary();

        if (data.isEmpty()) {
            return MyOptional.absent();
        } else {
            Record record = data.removeFirst();
            checkpoint = checkpoint.moveAfter(record.getSequenceNumber());
            return MyOptional.of(record);
        }
    }

    private void readMoreIfNecessary() throws IOException {
        if (data.isEmpty()) {
            LOG.debug("Sending request for more data to Kinesis");

            GetRecordsResult response;
            try {
                response = kinesis.get().getRecords(shardIterator);
            } catch (ExpiredIteratorException e) {
                LOG.info("Refreshing expired iterator", e);
                shardIterator = checkpoint.getShardIterator(kinesis);
                response = kinesis.get().getRecords(shardIterator);
            }
            LOG.debug("Fetched {} new records", response.getRecords().size());
            if (response.getRecords().size() > 0) {
                shardIterator = response.getNextShardIterator();
                data.addAll(response.getRecords());
            }
        }
    }

    public SingleShardCheckpoint getCheckpoint() {
        return checkpoint;
    }
}
