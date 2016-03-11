package com.google.cloud.dataflow.sdk.io.kinesis.source;

import static com.google.cloud.dataflow.sdk.repackaged.com.google.common.base.Preconditions
        .checkNotNull;
import static com.google.cloud.dataflow.sdk.repackaged.com.google.common.collect.Queues
        .newArrayDeque;
import com.google.cloud.dataflow.sdk.io.kinesis.client.SimplifiedKinesisClient;
import com.google.cloud.dataflow.sdk.io.kinesis.source.checkpoint.SingleShardCheckpoint;
import com.google.cloud.dataflow.sdk.repackaged.com.google.common.base.MyOptional;
import com.google.cloud.dataflow.sdk.repackaged.com.google.common.base.Optional;

import com.amazonaws.services.kinesis.model.ExpiredIteratorException;
import com.amazonaws.services.kinesis.model.GetRecordsResult;
import com.amazonaws.services.kinesis.model.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.util.Deque;

/***
 *
 */
public class ShardRecordsIterator {
    private static final Logger LOG = LoggerFactory.getLogger(ShardRecordsIterator.class);

    private final SimplifiedKinesisClient kinesis;
    private final RecordTransformer transformer;
    private SingleShardCheckpoint checkpoint;
    private String shardIterator;
    private Deque<Record> data = newArrayDeque();

    public ShardRecordsIterator(final SingleShardCheckpoint initialCheckpoint,
                                SimplifiedKinesisClient simplifiedKinesisClient) throws
            IOException {
        this(initialCheckpoint, simplifiedKinesisClient, new RecordTransformer());
    }

    public ShardRecordsIterator(final SingleShardCheckpoint initialCheckpoint,
                                SimplifiedKinesisClient simplifiedKinesisClient,
                                RecordTransformer transformer) throws
            IOException {
        checkNotNull(initialCheckpoint);
        checkNotNull(simplifiedKinesisClient);

        this.checkpoint = initialCheckpoint;
        this.transformer = transformer;
        this.kinesis = simplifiedKinesisClient;
        shardIterator = checkpoint.getShardIterator(kinesis);
    }

    public Optional<Record> next() throws IOException {
        readMoreIfNecessary();

        if (data.isEmpty()) {
            return MyOptional.absent();
        } else {
            Record record = data.removeFirst();
            checkpoint = checkpoint.moveAfter(record);
            return MyOptional.of(record);
        }
    }

    private void readMoreIfNecessary() throws IOException {
        if (data.isEmpty()) {
            GetRecordsResult response;
            try {
                response = kinesis.getRecords(shardIterator);
            } catch (ExpiredIteratorException e) {
                LOG.info("Refreshing expired iterator", e);
                shardIterator = checkpoint.getShardIterator(kinesis);
                response = kinesis.getRecords(shardIterator);
            }
            LOG.debug("Fetched {} new records", response.getRecords().size());
            shardIterator = response.getNextShardIterator();
            if (response.getRecords().size() > 0) {
                data.addAll(transformer.transform(response.getRecords(), checkpoint));
            }
        }
    }

    public SingleShardCheckpoint getCheckpoint() {
        return checkpoint;
    }


}
