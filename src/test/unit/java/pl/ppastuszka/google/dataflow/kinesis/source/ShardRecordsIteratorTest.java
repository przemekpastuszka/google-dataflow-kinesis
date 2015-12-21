package pl.ppastuszka.google.dataflow.kinesis.source;

import com.google.cloud.dataflow.sdk.repackaged.com.google.common.base.MyOptional;
import com.google.cloud.dataflow.sdk.repackaged.com.google.common.base.Optional;

import com.amazonaws.services.kinesis.clientlibrary.exceptions.InvalidStateException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ShutdownException;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.ICheckpoint;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.SingleThreadShardConsumer;
import com.amazonaws.services.kinesis.clientlibrary.proxies.IKinesisProxy;
import com.amazonaws.services.kinesis.clientlibrary.proxies.KinesisProxy;
import com.amazonaws.services.kinesis.clientlibrary.types.ExtendedSequenceNumber;
import com.amazonaws.services.kinesis.clientlibrary.types.ProcessRecordsInput;
import com.amazonaws.services.kinesis.model.GetRecordsResult;
import com.amazonaws.services.kinesis.model.Record;
import static org.fest.assertions.Assertions.assertThat;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.when;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.runners.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;
import static java.util.Arrays.asList;
import java.io.IOException;
import java.util.Collections;
import pl.ppastuszka.google.dataflow.kinesis.client.SerializableKinesisProxyFactory;
import pl.ppastuszka.google.dataflow.kinesis.source.checkpoint.SingleShardCheckpoint;

/**
 * Created by ppastuszka on 12.12.15.
 */
@RunWith(MockitoJUnitRunner.class)
public class ShardRecordsIteratorTest {
    @Mock
    private SerializableKinesisProxyFactory kinesisProvider;
    @Mock
    private KinesisProxy kinesisProxy;
    @Mock
    private SingleShardCheckpoint firstCheckpoint, aCheckpoint, bCheckpoint, cCheckpoint,
            dCheckpoint;
    @Mock
    private GetRecordsResult data;
    @Mock
    private ExtendedSequenceNumber LATEST, A_SEQUENCE, B_SEQUENCE, C_SEQUENCE, D_SEQUENCE;
    @Mock
    private Record a, b, c, d;
    @Mock
    private IRecordProcessorCheckpointer checkpointer;

    private ShardRecordsIterator iterator;

    @Before
    public void setUp() throws IOException, InvalidStateException, ShutdownException {
        when(kinesisProvider.getProxy(anyString())).thenReturn(kinesisProxy);

        when(firstCheckpoint.getExtendedSequenceNumber()).thenReturn(LATEST);
        when(firstCheckpoint.moveAfter(LATEST)).thenReturn(firstCheckpoint);
        when(firstCheckpoint.moveAfter(A_SEQUENCE)).thenReturn(aCheckpoint);
        when(aCheckpoint.moveAfter(B_SEQUENCE)).thenReturn(bCheckpoint);
        when(bCheckpoint.moveAfter(C_SEQUENCE)).thenReturn(cCheckpoint);
        when(cCheckpoint.moveAfter(D_SEQUENCE)).thenReturn(dCheckpoint);

        when(data.getRecords()).thenReturn(Collections.<Record>emptyList());

        iterator = new ShardRecordsIterator(firstCheckpoint, kinesisProvider, new
                TestKinesisConsumerFactory());
    }

    @Test
    public void returnsAbsentIfNoRecordsPresent() throws IOException {
        assertThat(iterator.next()).isEqualTo(MyOptional.absent());
        assertThat(iterator.next()).isEqualTo(MyOptional.absent());
        assertThat(iterator.next()).isEqualTo(MyOptional.absent());
    }

    @Test
    public void goesThroughAvailableRecords() throws IOException {
        when(data.getRecords()).thenReturn(asList(a, b, c)).thenReturn(asList(d)).thenReturn
                (Collections.<Record>emptyList());

        assertThat(iterator.getCheckpoint()).isEqualTo(firstCheckpoint);
        assertThat(iterator.next()).isEqualTo(Optional.of(a));
        assertThat(iterator.getCheckpoint()).isEqualTo(aCheckpoint);
        assertThat(iterator.next()).isEqualTo(Optional.of(b));
        assertThat(iterator.getCheckpoint()).isEqualTo(bCheckpoint);
        assertThat(iterator.next()).isEqualTo(Optional.of(c));
        assertThat(iterator.getCheckpoint()).isEqualTo(cCheckpoint);
        assertThat(iterator.next()).isEqualTo(Optional.of(d));
        assertThat(iterator.getCheckpoint()).isEqualTo(dCheckpoint);
        assertThat(iterator.next()).isEqualTo(MyOptional.absent());
        assertThat(iterator.getCheckpoint()).isEqualTo(dCheckpoint);
    }

    private class TestKinesisConsumerFactory extends KinesisShardConsumerFactory {
        SingleThreadShardConsumer getConsumer(final String shardId, IKinesisProxy proxy, final
        ICheckpoint
                checkpoint, final IRecordProcessor recordProcessor) {

            return new SingleThreadShardConsumer(shardId, proxy, checkpoint, recordProcessor) {
                @Override
                public void consume() {
                    try {
                        doAnswer(getAnswer(A_SEQUENCE)).when(checkpointer).checkpoint(a);
                        doAnswer(getAnswer(B_SEQUENCE)).when(checkpointer).checkpoint(b);
                        doAnswer(getAnswer(C_SEQUENCE)).when(checkpointer).checkpoint(c);
                        doAnswer(getAnswer(D_SEQUENCE)).when(checkpointer).checkpoint(d);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }

                    recordProcessor.processRecords(new ProcessRecordsInput().
                                    withRecords(data.getRecords()).withCheckpointer(checkpointer
                            )
                    );
                }

                private Answer getAnswer(final ExtendedSequenceNumber extendedSequenceNumber) {
                    return new Answer() {
                        @Override
                        public Object answer(InvocationOnMock invocation) throws Throwable {
                            checkpoint.setCheckpoint(shardId, extendedSequenceNumber, null);
                            return null;
                        }
                    };
                }
            };
        }
    }

}