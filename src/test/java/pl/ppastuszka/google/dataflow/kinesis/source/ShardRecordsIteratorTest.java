package pl.ppastuszka.google.dataflow.kinesis.source;

import com.google.cloud.dataflow.sdk.repackaged.com.google.common.base.MyOptional;
import com.google.cloud.dataflow.sdk.repackaged.com.google.common.base.Optional;

import com.amazonaws.services.kinesis.model.ExpiredIteratorException;
import com.amazonaws.services.kinesis.model.GetRecordsResult;
import com.amazonaws.services.kinesis.model.Record;
import static org.fest.assertions.Assertions.assertThat;
import static org.mockito.Mockito.when;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import static java.util.Arrays.asList;
import java.io.IOException;
import java.util.Collections;
import pl.ppastuszka.google.dataflow.kinesis.client.SimplifiedKinesisClient;
import pl.ppastuszka.google.dataflow.kinesis.client.provider.KinesisClientProvider;
import pl.ppastuszka.google.dataflow.kinesis.source.checkpoint.SingleShardCheckpoint;

/**
 * Created by ppastuszka on 12.12.15.
 */
@RunWith(MockitoJUnitRunner.class)
public class ShardRecordsIteratorTest {
    public static final String INITIAL_ITERATOR = "INITIAL_ITERATOR";
    public static final String SECOND_ITERATOR = "SECOND_ITERATOR";
    public static final String SECOND_REFRESHED_ITERATOR = "SECOND_REFRESHED_ITERATOR";
    public static final String THIRD_ITERATOR = "THIRD_ITERATOR";
    public static final String A_SEQUENCE = "a sequence";
    public static final String B_SEQUENCE = "b sequence";
    public static final String C_SEQUENCE = "c sequence";
    public static final String D_SEQUENCE = "d sequence";

    @Mock
    private KinesisClientProvider kinesisProvider;
    @Mock
    private SimplifiedKinesisClient kinesisClient;
    @Mock
    private SingleShardCheckpoint firstCheckpoint, aCheckpoint, bCheckpoint, cCheckpoint,
            dCheckpoint;
    @Mock
    private GetRecordsResult firstResult, secondResult, thirdResult;
    @Mock
    private Record a, b, c, d;

    private ShardRecordsIterator iterator;

    @Before
    public void setUp() throws IOException {
        when(kinesisProvider.get()).thenReturn(kinesisClient);
        when(firstCheckpoint.getShardIterator(kinesisProvider)).thenReturn(INITIAL_ITERATOR);

        when(firstCheckpoint.moveAfter(A_SEQUENCE)).thenReturn(aCheckpoint);
        when(aCheckpoint.moveAfter(B_SEQUENCE)).thenReturn(bCheckpoint);
        when(bCheckpoint.moveAfter(C_SEQUENCE)).thenReturn(cCheckpoint);
        when(cCheckpoint.moveAfter(D_SEQUENCE)).thenReturn(dCheckpoint);

        when(kinesisClient.getRecords(INITIAL_ITERATOR)).thenReturn(firstResult);
        when(kinesisClient.getRecords(SECOND_ITERATOR)).thenReturn(secondResult);
        when(kinesisClient.getRecords(THIRD_ITERATOR)).thenReturn(thirdResult);

        when(firstResult.getNextShardIterator()).thenReturn(SECOND_ITERATOR);
        when(secondResult.getNextShardIterator()).thenReturn(THIRD_ITERATOR);
        when(thirdResult.getNextShardIterator()).thenReturn(THIRD_ITERATOR);

        when(firstResult.getRecords()).thenReturn(Collections.<Record>emptyList());
        when(secondResult.getRecords()).thenReturn(Collections.<Record>emptyList());
        when(thirdResult.getRecords()).thenReturn(Collections.<Record>emptyList());

        when(a.getSequenceNumber()).thenReturn(A_SEQUENCE);
        when(b.getSequenceNumber()).thenReturn(B_SEQUENCE);
        when(c.getSequenceNumber()).thenReturn(C_SEQUENCE);
        when(d.getSequenceNumber()).thenReturn(D_SEQUENCE);

        iterator = new ShardRecordsIterator(firstCheckpoint, kinesisProvider);
    }

    @Test
    public void returnsAbsentIfNoRecordsPresent() throws IOException {
        assertThat(iterator.next()).isEqualTo(MyOptional.absent());
        assertThat(iterator.next()).isEqualTo(MyOptional.absent());
        assertThat(iterator.next()).isEqualTo(MyOptional.absent());
    }

    @Test
    public void goesThroughAvailableRecords() throws IOException {
        when(firstResult.getRecords()).thenReturn(asList(a, b, c));
        when(secondResult.getRecords()).thenReturn(asList(d));

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

    @Test
    public void refreshesExpiredIterator() throws IOException {
        when(firstResult.getRecords()).thenReturn(asList(a));
        when(secondResult.getRecords()).thenReturn(asList(b));

        when(kinesisClient.getRecords(SECOND_ITERATOR)).thenThrow(ExpiredIteratorException.class);
        when(aCheckpoint.getShardIterator(kinesisProvider)).thenReturn(SECOND_REFRESHED_ITERATOR);
        when(kinesisClient.getRecords(SECOND_REFRESHED_ITERATOR)).thenReturn(secondResult);

        assertThat(iterator.next()).isEqualTo(Optional.of(a));
        assertThat(iterator.next()).isEqualTo(Optional.of(b));
        assertThat(iterator.next()).isEqualTo(MyOptional.absent());
    }
}
