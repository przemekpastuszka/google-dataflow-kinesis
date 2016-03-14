package com.google.cloud.dataflow.sdk.io.kinesis.source;

import static com.google.cloud.dataflow.sdk.repackaged.com.google.common.base.Preconditions
        .checkNotNull;
import static com.google.cloud.dataflow.sdk.repackaged.com.google.common.collect.Queues
        .newArrayDeque;
import com.google.cloud.dataflow.sdk.io.kinesis.client.SimplifiedKinesisClient;
import com.google.cloud.dataflow.sdk.io.kinesis.source.checkpoint.ShardCheckpoint;
import com.google.cloud.dataflow.sdk.repackaged.com.google.common.base.CustomOptional;
import com.google.cloud.dataflow.sdk.repackaged.com.google.common.base.Optional;

import com.amazonaws.services.kinesis.clientlibrary.types.UserRecord;
import com.amazonaws.services.kinesis.model.ExpiredIteratorException;
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
    private ShardCheckpoint checkpoint;
    private String shardIterator;
    private Deque<UserRecord> data = newArrayDeque();

    public ShardRecordsIterator(final ShardCheckpoint initialCheckpoint,
                                SimplifiedKinesisClient simplifiedKinesisClient) throws
            IOException {
        this(initialCheckpoint, simplifiedKinesisClient, new RecordTransformer());
    }

    public ShardRecordsIterator(final ShardCheckpoint initialCheckpoint,
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

    public Optional<UserRecord> next() throws IOException {
        readMoreIfNecessary();

        if (data.isEmpty()) {
            return CustomOptional.absent();
        } else {
            UserRecord record = data.removeFirst();
            checkpoint = checkpoint.moveAfter(record);
            return CustomOptional.of(record);
        }
    }

    private void readMoreIfNecessary() throws IOException {
        if (data.isEmpty()) {
            SimplifiedKinesisClient.RecordsResult response;
            try {
                response = kinesis.getRecords(shardIterator);
            } catch (ExpiredIteratorException e) {
                LOG.info("Refreshing expired iterator", e);
                shardIterator = checkpoint.getShardIterator(kinesis);
                response = kinesis.getRecords(shardIterator);
            }
            LOG.debug("Fetched {} new records", response.getRecords().size());
            shardIterator = response.getNextShardIterator();
            data.addAll(transformer.transform(response.getRecords(), checkpoint));
        }
    }

    public ShardCheckpoint getCheckpoint() {
        return checkpoint;
    }


}
