package pl.ppastuszka.google.dataflow.kinesis.source;

import com.amazonaws.services.kinesis.model.Record;
import com.google.cloud.dataflow.sdk.io.UnboundedSource;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.repackaged.com.google.common.base.Function;
import com.google.cloud.dataflow.sdk.repackaged.com.google.common.base.MyOptional;
import com.google.cloud.dataflow.sdk.repackaged.com.google.common.base.Optional;
import com.google.cloud.dataflow.sdk.repackaged.com.google.common.collect.Lists;
import org.joda.time.Instant;
import pl.ppastuszka.google.dataflow.kinesis.client.provider.KinesisClientProvider;
import pl.ppastuszka.google.dataflow.kinesis.utils.RoundRobin;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.List;
import java.util.NoSuchElementException;

import static com.google.cloud.dataflow.sdk.repackaged.com.google.common.base.Preconditions.checkNotNull;
import static com.google.cloud.dataflow.sdk.repackaged.com.google.common.collect.Lists.transform;


/**
 * Created by ppastuszka on 05.12.15.
 */
public class KinesisReader extends UnboundedSource.UnboundedReader<byte[]> {
    private final KinesisClientProvider kinesis;
    private MultiShardCheckpoint initialCheckpoint;
    private final UnboundedSource<byte[], ?> source;
    private RoundRobin<ShardRecordsIterator> shardIterators;
    private Optional<Record> currentRecord = MyOptional.absent();

    public KinesisReader(KinesisClientProvider kinesis, MultiShardCheckpoint checkpointMark, PipelineOptions options,
                         UnboundedSource<byte[], ?> source) {
        checkNotNull(kinesis);
        checkNotNull(checkpointMark);

        this.kinesis = kinesis;
        this.source = source;
        this.initialCheckpoint = checkpointMark;
    }

    @Override
    public boolean start() throws IOException {
        shardIterators = new RoundRobin<ShardRecordsIterator>(transform(initialCheckpoint, new Function<SingleShardCheckpoint, ShardRecordsIterator>() {
            @Override
            public ShardRecordsIterator apply(SingleShardCheckpoint singleShardCheckpoint) {
                return singleShardCheckpoint.getShardRecordsIterator(kinesis);
            }
        }));

        return advance();
    }

    @Override
    public boolean advance() throws IOException {
        for (int i = 0; i < shardIterators.size(); ++i) {
            currentRecord = shardIterators.getCurrent().next();
            if (currentRecord.isPresent()) {
                break;
            } else {
                shardIterators.moveForward();
            }
        }
        return currentRecord.isPresent();
    }

    @Override
    public byte[] getCurrentRecordId() throws NoSuchElementException {
        return currentRecord.get().getSequenceNumber().getBytes();
    }

    @Override
    public byte[] getCurrent() throws NoSuchElementException {
        return currentRecord.get().getData().array();
    }

    @Override
    public Instant getCurrentTimestamp() throws NoSuchElementException {
        return new Instant(currentRecord.get().getApproximateArrivalTimestamp());
    }

    @Override
    public void close() throws IOException {
    }

    @Override
    public Instant getWatermark() {
        return getCurrentTimestamp();
    }

    @Override
    public UnboundedSource.CheckpointMark getCheckpointMark() {
        return new MultiShardCheckpoint(shardIterators);
    }

    @Override
    public UnboundedSource<byte[], ?> getCurrentSource() {
        return source;
    }

}
