package com.google.cloud.dataflow.sdk.io.kinesis.source;

import static com.google.cloud.dataflow.sdk.repackaged.com.google.common.base.Preconditions
        .checkNotNull;
import static com.google.cloud.dataflow.sdk.repackaged.com.google.common.collect.Lists.newArrayList;
import com.google.cloud.dataflow.sdk.io.UnboundedSource;
import com.google.cloud.dataflow.sdk.io.kinesis.client.SimplifiedKinesisClient;
import com.google.cloud.dataflow.sdk.io.kinesis.source.checkpoint.ShardCheckpoint;
import com.google.cloud.dataflow.sdk.io.kinesis.source.checkpoint.StreamCheckpoint;
import com.google.cloud.dataflow.sdk.io.kinesis.source.checkpoint.generator.CheckpointGenerator;
import com.google.cloud.dataflow.sdk.io.kinesis.utils.RoundRobin;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.repackaged.com.google.common.base.Charsets;
import com.google.cloud.dataflow.sdk.repackaged.com.google.common.base.CustomOptional;
import com.google.cloud.dataflow.sdk.repackaged.com.google.common.base.Optional;

import com.amazonaws.services.kinesis.clientlibrary.types.UserRecord;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.util.List;
import java.util.NoSuchElementException;


/***
 *
 */
public class KinesisReader extends UnboundedSource.UnboundedReader<byte[]> {
    private static final Logger LOG = LoggerFactory.getLogger(KinesisReader.class);

    private final SimplifiedKinesisClient kinesis;
    private final UnboundedSource<byte[], ?> source;
    private CheckpointGenerator initialCheckpointGenerator;
    private RoundRobin<ShardRecordsIterator> shardIterators;
    private Optional<UserRecord> currentRecord = CustomOptional.absent();

    public KinesisReader(SimplifiedKinesisClient kinesis,
                         CheckpointGenerator initialCheckpointGenerator,
                         PipelineOptions options,
                         UnboundedSource<byte[], ?> source) {
        checkNotNull(kinesis);
        checkNotNull(initialCheckpointGenerator);

        this.kinesis = kinesis;
        this.source = source;
        this.initialCheckpointGenerator = initialCheckpointGenerator;
    }

    @Override
    public boolean start() throws IOException {
        LOG.info("Starting reader using {}", initialCheckpointGenerator);

        StreamCheckpoint initialCheckpoint = initialCheckpointGenerator.generate(kinesis);
        List<ShardRecordsIterator> iterators = newArrayList();
        for (ShardCheckpoint checkpoint : initialCheckpoint) {
            iterators.add(checkpoint.getShardRecordsIterator(kinesis));
        }
        shardIterators = new RoundRobin<>(iterators);

        return advance();
    }

    @Override
    public boolean advance() throws IOException {
        for (int i = 0; i < shardIterators.size(); ++i) {
            currentRecord = shardIterators.getCurrent().next();
            if (currentRecord.isPresent()) {
                return true;
            } else {
                shardIterators.moveForward();
            }
        }
        return false;
    }

    @Override
    public byte[] getCurrentRecordId() throws NoSuchElementException {
        return currentRecord.get().getSequenceNumber().getBytes(Charsets.UTF_8);
    }

    @Override
    public byte[] getCurrent() throws NoSuchElementException {
        return currentRecord.get().getData().array();
    }

    @Override
    public Instant getCurrentTimestamp() throws NoSuchElementException {
        return Instant.now();
    }

    @Override
    public void close() throws IOException {
    }

    @Override
    public Instant getWatermark() {
        return Instant.now();
    }

    @Override
    public UnboundedSource.CheckpointMark getCheckpointMark() {
        return StreamCheckpoint.asCurrentStateOf(shardIterators);
    }

    @Override
    public UnboundedSource<byte[], ?> getCurrentSource() {
        return source;
    }

}
