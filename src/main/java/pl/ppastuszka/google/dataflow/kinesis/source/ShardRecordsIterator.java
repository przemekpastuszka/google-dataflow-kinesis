package pl.ppastuszka.google.dataflow.kinesis.source;

import static com.google.cloud.dataflow.sdk.repackaged.com.google.common.base.Preconditions
        .checkNotNull;
import static com.google.cloud.dataflow.sdk.repackaged.com.google.common.collect.Queues
        .newArrayDeque;
import com.google.cloud.dataflow.sdk.repackaged.com.google.common.base.MyOptional;
import com.google.cloud.dataflow.sdk.repackaged.com.google.common.base.Optional;

import com.amazonaws.services.kinesis.clientlibrary.exceptions.InvalidStateException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.KinesisClientLibException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ShutdownException;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.ICheckpoint;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.SingleThreadShardConsumer;
import com.amazonaws.services.kinesis.clientlibrary.types.ExtendedSequenceNumber;
import com.amazonaws.services.kinesis.clientlibrary.types.InitializationInput;
import com.amazonaws.services.kinesis.clientlibrary.types.ProcessRecordsInput;
import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownInput;
import com.amazonaws.services.kinesis.model.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.util.Deque;
import pl.ppastuszka.google.dataflow.kinesis.client.SerializableKinesisProxyFactory;
import pl.ppastuszka.google.dataflow.kinesis.source.checkpoint.SingleShardCheckpoint;

/***
 *
 */
public class ShardRecordsIterator {
    private static final Logger LOG = LoggerFactory.getLogger(ShardRecordsIterator.class);

    private final SerializableKinesisProxyFactory kinesis;
    private SingleShardCheckpoint checkpoint;
    private SingleThreadShardConsumer consumer;

    private Deque<Record> data = newArrayDeque();
    private IRecordProcessorCheckpointer checkpointer;

    public ShardRecordsIterator(final SingleShardCheckpoint initialCheckpoint,
                                SerializableKinesisProxyFactory

                                        kinesisClientConfiguration) throws IOException {
        checkNotNull(initialCheckpoint);
        checkNotNull(kinesisClientConfiguration);
        this.checkpoint = initialCheckpoint;
        this.kinesis = kinesisClientConfiguration;

        ICheckpoint checkpointProcessor = new ICheckpoint() {
            @Override
            public void setCheckpoint(String shardId, ExtendedSequenceNumber
                    checkpointValue, String concurrencyToken) throws
                    KinesisClientLibException {
                ShardRecordsIterator.this.checkpoint = checkpoint.moveAfter(checkpointValue);
            }

            @Override
            public ExtendedSequenceNumber getCheckpoint(String shardId) throws
                    KinesisClientLibException {
                return ShardRecordsIterator.this.checkpoint.getExtendedSequenceNumber();
            }
        };
        consumer = new SingleThreadShardConsumer(
                initialCheckpoint.shardId,
                kinesis.getProxy(initialCheckpoint.streamName),
                checkpointProcessor,
                new IRecordProcessor() {
                    @Override
                    public void initialize(InitializationInput initializationInput) {

                    }

                    @Override
                    public void processRecords(ProcessRecordsInput processRecordsInput) {
                        data.addAll(processRecordsInput.getRecords());
                        checkpointer = processRecordsInput.getCheckpointer();
                    }

                    @Override
                    public void shutdown(ShutdownInput shutdownInput) {

                    }
                }
        );
        try {
            checkpointProcessor.setCheckpoint(checkpoint.shardId, checkpoint
                    .getExtendedSequenceNumber(), null);
        } catch (KinesisClientLibException e) {
            throw new RuntimeException(e);
        }
        consumer.consume();
        consumer.consume();
    }

    public Optional<Record> next() throws IOException {
        readMoreIfNecessary();

        if (data.isEmpty()) {
            return MyOptional.absent();
        } else {
            Record record = data.removeFirst();
            try {
                checkpointer.checkpoint(record);
            } catch (InvalidStateException | ShutdownException e) {
                throw new RuntimeException(e);
            }
            return MyOptional.of(record);
        }
    }

    private void readMoreIfNecessary() throws IOException {
        if (data.isEmpty()) {
            consumer.consume();
        }
    }

    public SingleShardCheckpoint getCheckpoint() {
        return checkpoint;
    }
}
