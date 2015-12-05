import com.amazonaws.AmazonClientException;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.model.GetRecordsRequest;
import com.amazonaws.services.kinesis.model.GetRecordsResult;
import com.amazonaws.services.kinesis.model.Record;
import com.google.cloud.dataflow.sdk.io.UnboundedSource;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.repackaged.com.google.common.collect.Queues;
import org.joda.time.Instant;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.NoSuchElementException;

import static com.amazonaws.services.kinesis.model.ShardIteratorType.AFTER_SEQUENCE_NUMBER;

/**
 * Created by ppastuszka on 05.12.15.
 */
public class KinesisReader extends UnboundedSource.UnboundedReader<byte[]> {
    private final AmazonKinesis kinesis;
    private final UnboundedSource<byte[], ?> source;
    private final KinesisCheckpoint initialCheckpoint;
    private String shardIterator;
    private String lastSequenceNumber;
    private ArrayDeque<Record> data = Queues.newArrayDeque();

    public KinesisReader(AmazonKinesis kinesis, String streamName, String shardId, KinesisCheckpoint checkpointMark, PipelineOptions options, UnboundedSource<byte[], ?> source) {
        this.kinesis = kinesis;
        this.source = source;
        this.initialCheckpoint = checkpointMark;
        this.shardIterator = kinesis.getShardIterator(streamName, shardId, checkpointMark.getShardIteratorType(), checkpointMark.getSequenceNumber()).getShardIterator();
    }

    @Override
    public boolean start() throws IOException {
        readMore();
        return !data.isEmpty();
    }

    private void readMore() throws IOException {
        try {
            GetRecordsRequest getRecordsRequest = new GetRecordsRequest();
            GetRecordsResult response = kinesis.getRecords(getRecordsRequest.withShardIterator(shardIterator));
            shardIterator = response.getNextShardIterator();
            data.addAll(response.getRecords());
        } catch (AmazonClientException ex) {
            throw new IOException(ex);
        }
    }

    @Override
    public boolean advance() throws IOException {
        if (data.isEmpty()) {
            readMore();
        }

        if (data.isEmpty()) {
            return false;
        } else {
            Record lastElement = data.removeFirst();
            lastSequenceNumber = lastElement.getSequenceNumber();
            return true;
        }
    }

    @Override
    public byte[] getCurrent() throws NoSuchElementException {
        return data.getFirst().getData().array();
    }

    @Override
    public Instant getCurrentTimestamp() throws NoSuchElementException {
        return new Instant(data.getFirst().getApproximateArrivalTimestamp());
    }

    @Override
    public void close() throws IOException {
        data.clear();
    }

    @Override
    public Instant getWatermark() {
        return getCurrentTimestamp();
    }

    @Override
    public UnboundedSource.CheckpointMark getCheckpointMark() {
        if (data.isEmpty()) {
            if (lastSequenceNumber != null) {
                return new KinesisCheckpoint(AFTER_SEQUENCE_NUMBER.toString(), lastSequenceNumber);
            } else {
                return initialCheckpoint;
            }
        }
        return new KinesisCheckpoint(AFTER_SEQUENCE_NUMBER.toString(), data.getFirst().getSequenceNumber());
    }

    @Override
    public UnboundedSource<byte[], ?> getCurrentSource() {
        return source;
    }

}
