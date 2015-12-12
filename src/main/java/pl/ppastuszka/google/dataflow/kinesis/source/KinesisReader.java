package pl.ppastuszka.google.dataflow.kinesis.source;

import com.amazonaws.services.kinesis.model.Record;
import com.google.cloud.dataflow.sdk.io.UnboundedSource;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.repackaged.com.google.common.base.Optional;
import org.joda.time.Instant;
import pl.ppastuszka.google.dataflow.kinesis.client.provider.KinesisClientProvider;

import java.io.IOException;
import java.util.NoSuchElementException;

import static com.google.cloud.dataflow.sdk.repackaged.com.google.common.base.Preconditions.checkNotNull;


/**
 * Created by ppastuszka on 05.12.15.
 */
public class KinesisReader extends UnboundedSource.UnboundedReader<byte[]> {
    private final KinesisClientProvider kinesis;
    private KinesisCheckpoint initialCheckpoint;
    private final UnboundedSource<byte[], ?> source;
    private ShardRecordsIterator shardIterator;
    private Optional<Record> currentRecord = Optional.absent();

    public KinesisReader(KinesisClientProvider kinesis, KinesisCheckpoint checkpointMark, PipelineOptions options,
                         UnboundedSource<byte[], ?> source) {
        checkNotNull(kinesis);
        checkNotNull(checkpointMark);

        this.kinesis = kinesis;
        this.source = source;
        this.initialCheckpoint = checkpointMark;
    }

    @Override
    public boolean start() throws IOException {
        shardIterator = new ShardRecordsIterator(initialCheckpoint, kinesis);

        return advance();
    }

    @Override
    public boolean advance() throws IOException {
        currentRecord = shardIterator.next();
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
        return shardIterator.getCheckpoint();
    }

    @Override
    public UnboundedSource<byte[], ?> getCurrentSource() {
        return source;
    }

}
